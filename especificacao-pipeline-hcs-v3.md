# Pipeline Distribuído para Análise de Imagem de High-Content Screening

**Especificação técnica — v3**
Projeto de aprendizagem. Não se destina a produção clínica.

---

## Alterações face à v2

| # | Alteração | Motivo |
|---|---|---|
| 1 | **Política de bibliotecas invertida** (§3) | Reinventar mecanismos deixou de ser regra e passou a ser exceção justificada |
| 2 | **`fields` como unidade de processamento** (§6) | *Correção de bug.* Os três canais de um campo têm de ser processados juntos — segmenta-se no ADN, mede-se nos outros |
| 3 | **Tiling via `dask.array.map_overlap`** (§8.1) | A biblioteca já faz o chunking, a sobreposição e a preguiça. Sobra a política de labels, que é o que interessa |
| 4 | **Backpressure como mecanismo** (§10) | Era métrica sem mecanismo. Fechava o Objetivo A pela metade |
| 5 | **Linhagem e reprodutibilidade** (§12) | Um resultado científico que não se consegue reproduzir não é resultado |
| 6 | **Capítulo de testes** (§13) | Propriedades do tiling, lote dourado, exatidão contra ground truth |
| 7 | **Camada científica real** (§9) | Perfilagem, IC50 e classificação de mecanismo de ação |
| 8 | **Segredos fora do compose, CI/CD** (§15, §16) | Higiene básica, barata |

---

## 1. O que isto é

Um pipeline distribuído que ingere imagens de microscopia de fluorescência em lote, segmenta células, extrai perfis morfológicos, agrega-os por poço e placa, e conclui com **identificação de hits e classificação de mecanismo de ação** — com controlo explícito de memória, tolerância a falhas de worker, backpressure e telemetria.

Validado contra o **BBBC021**: 13.200 imagens reais de células MCF-7 tratadas com 113 compostos, três canais (ADN, F-actina, β-tubulina), com rótulos de mecanismo de ação para 103 combinações composto-concentração.

### 1.1 Objetivos de aprendizagem

Estes são o produto. As funcionalidades existem ao serviço deles.

| # | Conceito | Como é exercitado | Como é provado |
|---|---|---|---|
| A | Ingestão assíncrona e backpressure | `202` imediato; `429` acima do limiar de fila | p95 do POST < 200 ms; fila nunca excede o limiar |
| B | Memória constante sob input variável | Leitura chunked e preguiçosa, nunca o ficheiro inteiro | RSS plano de 100 MB a 8 GB |
| C | Semântica de falhas | Retries tipados, idempotência, DLQ, reconciliação | 0 duplicados após teste de caos |
| D | Paralelização e batching | N workers + serviço de inferência com batching | Speedup medido, comparado com Triton |
| E | Observabilidade | Prometheus, Grafana, teste de carga | Painel que responde "onde está o estrangulamento?" |
| F | Correção de domínio | Z′, % inibição, IC50, classificação de MoA | Z′ > 0.5; MoA acima do acaso em not-same-compound |
| G | Reprodutibilidade | Linhagem completa por execução | Reprocessar um lote antigo dá bit-a-bit o mesmo |
| H | Rigor de teste | Propriedades, lote dourado, ground truth | Suite que falha quando deve falhar |

### 1.2 Fora de âmbito — decisões conscientes

- **Não é alta disponibilidade.** Corre num único host. Há réplicas da API atrás de um balanceador e N workers — isso é *escalabilidade horizontal* e *tolerância a falha de worker*, não HA. Postgres, Redis e MinIO são pontos únicos de falha e assim ficam.
- **Não é multi-tenant.** Autenticação por chave de API apenas.
- **Não é tempo real.** Assíncrono por construção; o dashboard mostra estado quase-imediato.
- **Sem Kubernetes, sem replicação de base de dados.** Complexidade que não serve nenhum objetivo da tabela acima.

---

## 2. Fluxo de trabalho

1. **Captura** — o microscópio fotografa cada campo de cada poço em três canais e escreve os ficheiros no armazenamento de objetos.
2. **Registo** — o software do microscópio envia um manifesto à API: paths, poços, campos, canais, hashes. Resposta em milissegundos com o UUID do lote.
3. **Processamento** — os workers consomem a fila. Por **campo** (não por ficheiro): leem os três canais em chunks, segmentam no canal de ADN, medem nos três, escrevem métricas por célula.
4. **Agregação** — quando uma placa termina: perfil por poço, normalização aos controlos DMSO, Z′, % de inibição.
5. **Análise** — curvas dose-resposta e IC50 por composto; classificação de mecanismo de ação por vizinhança no espaço de perfis.
6. **Visualização** — heatmap da placa, curvas, lista de hits, exportação para Parquet.

---

## 3. Política de bibliotecas

**Regra: usa a biblioteca.** O objetivo é perceber sistemas distribuídos e análise de imagem, não reescrever `scikit-image`. Código que não escreves é código que não tens de manter, testar nem depurar — e o tempo poupado vai para os objetivos que interessam.

**Exceção, e é estreita.** Implementa à mão apenas quando o mecanismo *é* o objetivo de aprendizagem. Nesse caso valem três condições, todas obrigatórias:

1. A implementação manual fica atrás da mesma interface que a biblioteca.
2. Existe um benchmark que compara as duas.
3. A biblioteca é o caminho por omissão em execução normal; a versão manual é uma opção de configuração.

Neste projeto **exatamente um** componente cumpre a exceção: o **micro-batching de inferência** (§11). Tudo o resto usa biblioteca.

O que isto muda na prática face à v2: o tiler manual sai do caminho crítico e passa a apêndice opcional; o `map_overlap` do Dask entra; o Triton passa a caminho por omissão em vez de comparação final.

### 3.1 Stack

| Preocupação | Biblioteca | Nota |
|---|---|---|
| API | FastAPI, Pydantic v2, pydantic-settings | Validação e configuração declarativas |
| Persistência | SQLAlchemy 2.0, Alembic | Migrações desde o primeiro commit |
| Fila | Celery + Redis | Ver ADR-001 |
| Objetos | MinIO, `s3fs` | `s3fs` dá acesso tipo-ficheiro, integra com Zarr |
| Leitura de imagem | **`bioio`** (+ `bioio-ome-tiff`, `bioio-ome-zarr`), `tifffile`, `openslide-python` | Sistema de leitores por plugin; devolve arrays 5D TCZYX e suporta Dask |
| Computação preguiçosa | **`dask.array`** | `map_overlap` resolve chunking + sobreposição |
| Segmentação clássica | `scikit-image` | Otsu, watershed, `regionprops_table` |
| Segmentação DL | `cellpose`, `stardist` | StarDist mais rápido, assume núcleos estrelado-convexos; Cellpose melhor em células alongadas |
| Perfilagem | **`pycytominer`** | `aggregate`, `annotate`, `normalize`, `feature_select` + correção de efeito de lote. É a biblioteca do Broad, usada no JUMP e no LINCS |
| Estatística | `scipy`, `numpy`, `pandas` | `curve_fit` para a logística de 4 parâmetros |
| Serving | **Triton** ou **Ray Serve** | Batching dinâmico nativo |
| Métricas | `prometheus-fastapi-instrumentator`, `celery-exporter` | Instrumentação sem escrever coletores |
| Experiências | `mlflow` | Comparar backends e configurações é literalmente experiment tracking |
| Formatos | `pyarrow` | Parquet para exportação |
| Dashboard | `streamlit`, `plotly` | |
| Testes | `pytest`, `hypothesis`, `testcontainers`, `pytest-cov` | |
| Carga e caos | `locust`, `pumba` | |
| Qualidade | `ruff`, `mypy`, `trivy` | |

---

## 4. Arquitetura

```mermaid
flowchart TB
    subgraph Cliente
        MS[Microscópio / script]
        UI[Streamlit]
    end

    LB[Traefik]
    API1[FastAPI #1]
    API2[FastAPI #2]

    subgraph Estado
        PG[(PostgreSQL)]
        S3[(MinIO)]
        RQ[[Redis - broker]]
    end

    subgraph Computação
        W1[Worker Celery]
        W2[Worker Celery]
        W3[Worker Celery]
        INF[Triton / Ray Serve]
    end

    subgraph Telemetria
        PROM[Prometheus]
        GRAF[Grafana]
        FLO[Flower]
        ML[MLflow]
    end

    MS --> LB
    UI --> LB
    LB --> API1 & API2
    API1 & API2 --> PG
    API1 & API2 --> S3
    API1 & API2 --> RQ
    RQ --> W1 & W2 & W3
    W1 & W2 & W3 --> S3
    W1 & W2 & W3 --> PG
    W1 & W2 & W3 -.tiles.-> INF
    W1 & W2 & W3 --> ML
    API1 & API2 & W1 & INF --> PROM
    PROM --> GRAF
    RQ --> FLO
```

**Invariante:** a API nunca abre um ficheiro de imagem. Escreve bytes para o MinIO em streaming e linhas para o Postgres. Todo o trabalho pesado vive nos workers.

---

## 5. Correção estrutural: o campo é a unidade de processamento

Na v2 a tarefa era `process_image(image_id)`. Está errado, e o erro contamina tudo o resto.

O BBBC021 guarda os três canais de um campo como **ficheiros separados**. Não se pode segmentar um canal isoladamente: usa-se o canal de ADN para encontrar os núcleos, e os canais de actina e tubulina para *medir dentro* dessa máscara. Processar cada ficheiro em separado produz três conjuntos de medições que nunca se conseguem reconciliar ao nível da célula.

A unidade de processamento é o **campo** — o conjunto de N ficheiros que partilham `(placa, poço, campo)`. A tarefa é `process_field(field_id, run_id)`. O progresso conta-se em campos. A idempotência é por campo. Um campo a que falte um canal é rejeitado inteiro, não parcialmente processado.

---

## 6. Modelo de dados

```sql
CREATE TYPE batch_status AS ENUM
    ('DRAFT','QUEUED','RUNNING','COMPLETED','PARTIAL','FAILED','CANCELLED');
CREATE TYPE field_status AS ENUM
    ('PENDING','PROCESSING','DONE','FAILED','QC_REJECTED','INCOMPLETE');

CREATE TABLE batches (
    id               UUID PRIMARY KEY,
    external_ref     TEXT,
    status           batch_status NOT NULL DEFAULT 'DRAFT',
    total_fields     INT NOT NULL DEFAULT 0,
    processed_fields INT NOT NULL DEFAULT 0,
    failed_fields    INT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    queued_at        TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ
);

-- Linhagem: uma execução do pipeline sobre um lote, com contexto reproduzível.
CREATE TABLE runs (
    id              UUID PRIMARY KEY,
    batch_id        UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    pipeline_version TEXT NOT NULL,          -- 'classic-0.3.1' | 'cellpose-2.1.0'
    image_digest    TEXT NOT NULL,           -- sha256 da imagem de contentor
    config_hash     CHAR(64) NOT NULL,       -- sha256 da config canonicalizada
    config          JSONB NOT NULL,
    random_seed     BIGINT NOT NULL,
    mlflow_run_id   TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);
CREATE INDEX runs_batch_ix ON runs (batch_id, started_at DESC);

CREATE TABLE plates (
    id       UUID PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    barcode  TEXT NOT NULL,
    n_rows   SMALLINT NOT NULL,              -- 8 (96 poços) | 16 (384)
    n_cols   SMALLINT NOT NULL,              -- 12           | 24
    UNIQUE (batch_id, barcode)
);

CREATE TABLE well_layout (
    plate_id      UUID NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    well          TEXT NOT NULL,             -- 'B04'
    compound      TEXT,
    concentration REAL,
    moa           TEXT,                      -- rótulo de referência, quando existe
    control_type  TEXT,                      -- NULL | 'NEGATIVE' (DMSO) | 'POSITIVE'
    PRIMARY KEY (plate_id, well)
);

-- A unidade de processamento.
CREATE TABLE fields (
    id            UUID PRIMARY KEY,
    batch_id      UUID NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    plate_id      UUID NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    well          TEXT NOT NULL,
    field_index   SMALLINT NOT NULL,
    status        field_status NOT NULL DEFAULT 'PENDING',
    attempts      SMALLINT NOT NULL DEFAULT 0,
    last_error    TEXT,
    error_class   TEXT,                      -- 'TRANSIENT' | 'PERMANENT'
    qc            JSONB,                     -- {"focus_score":184.2,"blurry":false}
    processed_at  TIMESTAMPTZ,
    UNIQUE (plate_id, well, field_index)
);
CREATE INDEX fields_batch_status_ix ON fields (batch_id, status);

-- Um ficheiro = um canal de um campo.
CREATE TABLE images (
    id             UUID PRIMARY KEY,
    field_id       UUID NOT NULL REFERENCES fields(id) ON DELETE CASCADE,
    channel        TEXT NOT NULL,            -- 'DNA' | 'ACTIN' | 'TUBULIN'
    storage_uri    TEXT NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    width INT, height INT, size_bytes BIGINT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (field_id, channel)
);
CREATE UNIQUE INDEX images_content_uk ON images (content_sha256);

CREATE TABLE cell_metrics (
    id               BIGSERIAL PRIMARY KEY,
    field_id         UUID NOT NULL REFERENCES fields(id) ON DELETE CASCADE,
    run_id           UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    label            INT NOT NULL,
    centroid_x REAL, centroid_y REAL,
    area REAL, perimeter REAL, eccentricity REAL, solidity REAL,
    touches_border   BOOLEAN NOT NULL DEFAULT FALSE,
    features         JSONB NOT NULL,         -- intensidades por canal, textura, etc.
    -- Chave de idempotência: reprocessar faz UPSERT, não duplica.
    UNIQUE (field_id, run_id, label)
);
CREATE INDEX cell_metrics_run_ix ON cell_metrics (run_id, field_id);

CREATE TABLE well_profiles (
    plate_id           UUID NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    well               TEXT NOT NULL,
    run_id             UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    cell_count         INT,
    profile            JSONB NOT NULL,       -- vetor de features normalizado
    percent_inhibition REAL,
    PRIMARY KEY (plate_id, well, run_id)
);

CREATE TABLE plate_qc (
    plate_id    UUID NOT NULL REFERENCES plates(id) ON DELETE CASCADE,
    run_id      UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    z_prime     REAL,
    cv_negative REAL,
    n_negative INT, n_positive INT,
    passed      BOOLEAN,
    PRIMARY KEY (plate_id, run_id)
);

CREATE TABLE dose_response (
    compound   TEXT NOT NULL,
    run_id     UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    ic50       REAL,
    hill_slope REAL,
    top REAL, bottom REAL,
    r_squared  REAL,
    n_points   SMALLINT,
    is_hit     BOOLEAN,
    PRIMARY KEY (compound, run_id)
);
```

### Notas de desenho

**`run_id` substitui `algorithm_version` em todas as tabelas de resultado.** Ganho: um resultado aponta para o contexto completo — versão, digest da imagem de contentor, configuração, seed. Reprocessar cria um `run` novo e os dois coexistem para comparação.

**Não há tabela de tiles.** Um ficheiro de 4 GB com chunks de 512² gera ~16 mil tiles; persistir cada um multiplicaria as linhas por milhares sem responder a nenhuma pergunta. Os tiles são efémeros; o que persiste são as células. A contagem vive nas métricas.

**`features` em `JSONB` em vez de colunas fixas.** O conjunto de features muda entre backends — o clássico dá 20, o Cellpose com textura de Haralick dá centenas. Colunas rígidas obrigariam a uma migração por cada experiência. Custo aceite: consultas mais verbosas e sem índice por feature. Se uma feature se tornar central, promove-se a coluna.

**`content_sha256` é único globalmente.** O mesmo ficheiro em dois lotes é armazenado uma vez. Se preferires independência total por lote, muda para `UNIQUE (field_id, content_sha256)` — mas percebe o custo em armazenamento.

---

## 7. API

Rotas sob `/v1`. Erros em `application/problem+json` (RFC 9457). Autenticação por chave de API no cabeçalho `X-API-Key`.

### 7.1 Ciclo de vida

```http
POST /v1/batches
{ "external_ref": "plate_8291" }
201 { "batch_id": "0f9c…", "status": "DRAFT" }
```

**Modo A — manifesto por referência (recomendado).** O microscópio já escreveu no MinIO; passam metadados, não bytes. É isto que torna honesta a promessa de "10.000 imagens aceites em segundos".

```http
POST /v1/batches/{id}/manifest
{
  "plate": { "barcode": "8291", "n_rows": 16, "n_cols": 24 },
  "fields": [
    { "well": "B04", "field_index": 1,
      "channels": {
        "DNA":     { "uri": "s3://raw/8291/B04_f1_w1.tif", "sha256": "a1b2…" },
        "ACTIN":   { "uri": "s3://raw/8291/B04_f1_w2.tif", "sha256": "c3d4…" },
        "TUBULIN": { "uri": "s3://raw/8291/B04_f1_w4.tif", "sha256": "e5f6…" }
      } }
  ]
}
202 { "fields_registered": 3456, "duplicates_skipped": 12, "incomplete": 2 }
```

Campos a que falte um canal ficam `INCOMPLETE` e não são enfileirados. São reportados explicitamente — falhar em silêncio aqui produz estatísticas erradas mais à frente.

**Modo B — upload direto em streaming.** Para uso manual. Hash calculado *à medida* que os bytes passam para o MinIO, nunca em memória.

**Modo C — URLs pré-assinados.** A API sai do caminho dos dados.

### 7.2 Execução e consulta

```http
POST /v1/batches/{id}/submit      -> 202 { "run_id": "…" }  |  429 se a fila estiver saturada
POST /v1/batches/{id}/cancel      -> 202
POST /v1/batches/{id}/reprocess   -> 202, { "pipeline_version": "cellpose-2.1.0" }

GET  /v1/batches/{id}             -> progresso em campos, ETA
GET  /v1/batches/{id}/events      -> text/event-stream
GET  /v1/runs/{id}                -> linhagem completa: digest, config, seed
GET  /v1/runs/{id}/results?format=json|parquet
GET  /v1/runs/{id}/compare/{other_run_id}   -> deltas entre execuções
GET  /v1/plates/{id}/heatmap?metric=percent_inhibition&run_id=…
GET  /v1/compounds/{name}/dose-response?run_id=…
GET  /v1/fields/{id}/failures     -> histórico de tentativas, classe de erro

GET  /healthz  /readyz  /metrics
```

`/healthz` e `/readyz` separados não é cerimónia: o balanceador usa o segundo para retirar uma réplica de serviço sem a matar.

---

## 8. Pipeline de processamento

### 8.1 Leitura chunked — a biblioteca faz o trabalho

O `dask.array.map_overlap` resolve o que na v2 era um tiler manual: divide o array em chunks, acrescenta a margem de sobreposição, aplica a função e remove a margem. A leitura é preguiçosa e o consumo de memória é limitado por `chunk × workers`, não pelo tamanho do ficheiro.

```python
from bioio import BioImage
import dask.array as da

def segment_field(uris: dict[str, str], cfg) -> "pd.DataFrame":
    # bioio devolve arrays 5D TCZYX suportados por Dask -> nada é lido ainda.
    dna = BioImage(uris["DNA"]).get_image_dask_data("YX")
    dna = dna.rechunk({0: cfg.chunk, 1: cfg.chunk})

    labels = da.map_overlap(
        segment_chunk,          # devolve labels inteiros para o chunk
        dna,
        depth=cfg.overlap,      # > diâmetro da maior célula esperada
        boundary="reflect",
        trim=True,
        dtype="int32",
    )
    return measure(labels, uris, cfg)   # regionprops_table por canal
```

**O que a biblioteca não resolve, e é o que interessa perceber.** O `map_overlap` trata bem funções onde o valor de cada píxel depende só da vizinhança. Segmentação de instâncias não é isso: cada chunk gera os seus próprios rótulos, começando em 1, e a mesma célula na fronteira aparece rotulada de forma diferente em dois chunks. A biblioteca não pode reconciliar isto — a política é tua.

Política adotada, a mesma da v2 e por bons motivos: **um objeto pertence ao chunk que contém o seu centróide.** Objetos cujo centróide cai na margem de sobreposição são descartados, porque pertencem ao vizinho. Sem deduplicação posterior, sem contagens duplas, e a regra é testável como propriedade (§13.1).

Objetos que tocam a borda *exterior da imagem completa* marcam-se `touches_border`: contam para a contagem de células, mas excluem-se das estatísticas de morfologia porque estão truncados.

Para formatos de lâmina inteira (`.svs`, `.ndpi`) o caminho é `openslide-python`, que já expõe níveis piramidais e leitura por região. Para OME-Zarr, `bioio-ome-zarr` dá chunks nativos sem conversão. **Converter na ingestão para OME-Zarr é a opção que torna tudo o resto mais simples** — considera-a a sério antes de otimizar leitores de TIFF.

### 8.2 Camadas de análise

**QC primeiro, antes de gastar computação.** Variância do Laplaciano abaixo do limiar → desfocado. Intensidade mediana perto do fundo → poço vazio. Ambos marcam `QC_REJECTED` e o campo não segue. Poços na borda da placa marcam-se para efeito de evaporação.

**Segmentação.** Duas implementações atrás de `segment(chunk) -> labels`:

- `classic` — `scikit-image`: gaussiano → Otsu → `remove_small_objects` → transformada de distância → `watershed`. CPU, determinístico, sem GPU. É a linha de base e o caminho por omissão até a Fase 6.
- `cellpose` / `stardist` — deep learning, via serviço de inferência.

**Medição.** `skimage.measure.regionprops_table` sobre os labels, uma vez por canal: área, perímetro, excentricidade, solidez, intensidade média e integrada, razão núcleo/citoplasma (anel dilatado menos o núcleo). Opcionalmente textura de Haralick, que é onde os perfis ganham poder discriminativo para MoA.

---

## 9. Camada científica

É esta secção que separa um pipeline de ficheiros de um sistema de screening. Usa `pycytominer` — faz `aggregate`, `annotate`, `normalize` e `feature_select`, incluindo correção de efeito de lote, e é a biblioteca com que o Broad processou o JUMP e o LINCS.

### 9.1 Perfil por poço

```python
import pycytominer as pcm

well = pcm.aggregate(cells, strata=["plate", "well"], operation="median")
well = pcm.annotate(well, platemap_df=layout)
well = pcm.normalize(well, method="mad_robustize",
                     samples="control_type == 'NEGATIVE'")   # intra-placa
well = pcm.feature_select(well, operation=[
    "variance_threshold", "correlation_threshold",
    "drop_na_columns", "drop_outliers"])
```

A mediana e não a média: distribuições de células são assimétricas e contaminadas por artefactos de segmentação. **A normalização é sempre intra-placa e contra os controlos negativos da própria placa** — normalizar contra outra placa deixa os efeitos de lote destruírem a comparação.

### 9.2 Qualidade do ensaio

```
Z′ = 1 − 3(σ_pos + σ_neg) / |μ_pos − μ_neg|
```

`Z′ > 0.5` excelente; `0 < Z′ ≤ 0.5` marginal; `Z′ ≤ 0` inutilizável — os controlos sobrepõem-se e nenhum resultado da placa é de confiança. O dashboard **recusa mostrar resultados** de placas com `Z′ ≤ 0`. É o detalhe que faz um biólogo levar o sistema a sério.

### 9.3 Dose-resposta e IC50

O output real de um screening não é "% de inibição por poço" — é uma lista de hits com potência. O BBBC021 tem várias concentrações por composto, portanto os dados já lá estão.

Logística de 4 parâmetros, ajustada com `scipy.optimize.curve_fit`:

```
y(x) = bottom + (top − bottom) / (1 + (x / IC50)^hill)
```

Guarda também o `r_squared` e o `n_points`. Um IC50 de um ajuste com R² de 0.3 sobre quatro pontos não é um resultado — e sem esses campos ninguém repara. Critério de hit: `r_squared > 0.8` **e** `ic50` dentro da gama de concentrações testadas (extrapolar IC50 para fora da gama é a forma mais comum de produzir números bonitos e falsos).

### 9.4 Classificação de mecanismo de ação

A adição que muda o significado do projeto. Compostos com o mesmo mecanismo produzem assinaturas morfológicas semelhantes — se o teu pipeline for bom, isso é recuperável a partir dos perfis.

Protocolo padrão **not-same-compound**: para cada tratamento, classifica pelo vizinho mais próximo no espaço de perfis (correlação cosseno), **excluindo todas as réplicas do próprio composto**. Sem essa exclusão estás a medir reprodutibilidade técnica, não generalização biológica — o erro clássico deste benchmark.

```python
from sklearn.metrics.pairwise import cosine_similarity

sim = cosine_similarity(profiles)
np.fill_diagonal(sim, -np.inf)
for i, row in enumerate(sim):                       # mascarar mesmo composto
    row[compound_of == compound_of[i]] = -np.inf
predicted = moa_of[sim.argmax(axis=1)]
accuracy  = (predicted == moa_of).mean()
```

Reporta a exatidão contra a linha de base do acaso (a frequência da classe maioritária) e uma matriz de confusão. Se ficar acima do acaso de forma clara, reproduziste o resultado central dos trabalhos publicados sobre este conjunto — com a tua própria infraestrutura. Se ficar em cima do acaso, também aprendeste algo: o problema estará na segmentação, nas features ou na normalização, e a investigação de qual é excelente material para o relatório.

---

## 10. Backpressure

Estava em falta. Havia profundidade de fila como *métrica*, mas nenhum *mecanismo* — o sistema aceitava alegremente 500 mil tarefas e morria a esgotar a memória do Redis. Que é exatamente a saturação que o projeto se propõe evitar.

**Admissão na submissão.** `POST /submit` consulta a profundidade da fila. Acima de `QUEUE_HIGH_WATERMARK`, devolve `429` com `Retry-After` calculado a partir do throughput observado:

```http
429 Too Many Requests
Retry-After: 120
{ "type": "/errors/queue-saturated", "title": "Fila saturada",
  "queue_depth": 52000, "limit": 50000, "estimated_drain_seconds": 118 }
```

**Enfileiramento por blocos.** Um lote de 10.000 campos não entra todo de uma vez. Um despachante enfileira em blocos de `CHUNK_SIZE`, mantendo a fila entre marca baixa e marca alta. O lote fica `QUEUED` e progride sem nunca inundar o broker.

**Limite de memória do Redis.** `maxmemory` com `noeviction` — preferir falhas de escrita ruidosas a despejo silencioso de tarefas. Uma tarefa despejada é uma imagem que desaparece sem ninguém dar por isso.

**Rate limit na ingestão.** Limite por chave de API, para que um cliente em ciclo não monopolize a capacidade.

O teste de aceitação: submeter 10× a capacidade da fila e verificar que o sistema devolve `429`, mantém a profundidade abaixo do limite e não perde nem um campo.

---

## 11. Inferência e batching — a única exceção à política

Os workers **não** carregam o modelo. Enviam chunks e recebem labels. Um modelo em memória em vez de N, e chunks de workers diferentes agregam-se na mesma passagem.

**Caminho por omissão: Triton ou Ray Serve.** Ambos fazem batching dinâmico de forma nativa e configurável. Em execução normal é isto que corre.

```protobuf
dynamic_batching {
  preferred_batch_size: [ 16, 32 ]
  max_queue_delay_microseconds: 5000
}
```

**Implementação manual, atrás da mesma interface, como experiência.** Aqui a exceção da §3 aplica-se: o mecanismo é o objetivo.

```python
class MicroBatcher:
    async def _loop(self):
        while True:
            item = await self.q.get()                    # bloqueia até haver 1
            batch = [item]
            deadline = time.monotonic() + self.max_wait_ms / 1000
            while len(batch) < self.max_batch:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self.q.get(), timeout))
                except asyncio.TimeoutError:
                    break
            out = await run_in_threadpool(self.model.predict,
                                          np.stack([b.tile for b in batch]))
            for b, r in zip(batch, out):
                b.future.set_result(r)
```

**A janela só se paga com concorrência real.** Com um worker a enviar chunks em série, `max_wait_ms` é latência desperdiçada. Com 4+ workers a produzir milhares de chunks, a fila enche antes do prazo e o batch fecha por tamanho — é aí que o ganho aparece.

A experiência que fecha o projeto, registada no MLflow:

| Configuração | Chunks/s | p95 (ms) | Ocupação GPU |
|---|---|---|---|
| sem batching, 1 worker | | | |
| manual, 1 worker | | | |
| manual, 4 workers | | | |
| manual, 8 workers | | | |
| Triton, 4 workers | | | |
| Triton, 8 workers | | | |

Se o Triton ganhar, o resultado é "implementei-o, percebi-o, e uso a versão de produção" — que é a conclusão correta e honesta.

---

## 12. Reprodutibilidade e linhagem

Um resultado científico que não se consegue reproduzir não é resultado.

**Cada execução grava:** digest da imagem de contentor (não a tag — as tags mudam), hash sha256 da configuração canonicalizada, seed aleatória, versão do pipeline, hashes de todos os inputs, versões das dependências (o lockfile). Tudo em `runs`, espelhado no MlLflow.

**Determinismo.** Seed fixada em `numpy`, `random` e nos backends de DL. Para o caminho clássico, o resultado deve ser bit-a-bit idêntico entre execuções. Para o caminho GPU não será — as reduções em vírgula flutuante não comutam e a ordem depende do batching. **Declara isso**, não o escondas: o caminho clássico é reproduzível bit-a-bit, o caminho GPU é reproduzível dentro de uma tolerância que deves medir e documentar.

**Critério de aceitação de G:** reprocessar um lote de há três meses, com o mesmo `image_digest` e `config_hash`, produz `cell_metrics` idênticas. Um teste que corre em CI contra o lote dourado.

**Imutabilidade.** Nada em `cell_metrics` é atualizado depois de o run terminar. Correções criam um run novo. `GET /runs/{a}/compare/{b}` mostra os deltas — é isto que permite dizer "a versão 2 do algoritmo mudou a contagem em 4%" com números em vez de impressões.

---

## 13. Testes

### 13.1 Propriedades — o teste mais elegante que este projeto permite

A regra "cada objeto pertence a exatamente um chunk" é uma invariante geométrica, e invariantes testam-se com `hypothesis` em vez de com exemplos escolhidos à mão.

```python
from hypothesis import given, strategies as st

@given(h=st.integers(64, 4096), w=st.integers(64, 4096),
       chunk=st.integers(32, 512), overlap=st.integers(0, 64))
def test_cada_pixel_pertence_a_exatamente_um_core(h, w, chunk, overlap):
    """Sem buracos, sem sobreposições. Toda a classe de bugs de fronteira."""
    cores = list(core_regions(h, w, chunk, overlap))
    coverage = np.zeros((h, w), dtype=np.uint8)
    for y0, x0, y1, x1 in cores:
        coverage[y0:y1, x0:x1] += 1
    assert coverage.min() == 1 and coverage.max() == 1
```

Três linhas de propriedade em vez de vinte casos de exemplo, e apanha os casos que ninguém escreve à mão: imagem mais pequena que o chunk, sobreposição zero, dimensões não divisíveis, chunk maior que a imagem. É a alternativa a descobrir o bug seis semanas depois através de contagens de células estranhas.

Segunda propriedade, sobre o pipeline: **contagem invariante ao chunking.** Segmentar a mesma imagem sintética com `chunk=256` e `chunk=1024` tem de dar a mesma contagem de células, dentro de uma tolerância pequena. Se não der, a política de fronteira está errada.

### 13.2 Lote dourado

Vinte campos fixos com output conhecido, versionados no repositório. Qualquer alteração que desloque a contagem de células mais de 2% ou o IC50 mais de 10% falha o CI.

É assim que pipelines científicos evitam deriva silenciosa: o código continua a correr, os números mudam devagar, e ninguém repara. Quando o desvio é intencional, atualiza-se a referência **no mesmo commit** que causa a alteração — e o diff da referência torna-se documentação da mudança.

### 13.3 Integração com serviços reais

`testcontainers` levanta Postgres, Redis e MinIO reais por sessão de teste. Mocks de armazenamento de objetos escondem exatamente os bugs que interessa apanhar: paginação, consistência, timeouts, multipart.

### 13.4 Caos — critério de aceitação de C

```bash
docker compose up --scale worker=4 -d
python scripts/submit_batch.py --fields 500
sleep 20 && docker compose kill -s SIGKILL worker-2 worker-3
# Invariantes ao terminar:
#   sem duplicados em (field_id, run_id, label)
#   processed + failed == total
#   nenhum campo preso em PROCESSING
#   falhas permanentes com attempts == 1
```

Com `pumba` acrescentam-se latência e perda de pacotes entre worker e MinIO, que é como se descobre que faltam timeouts.

### 13.5 Exatidão contra ground truth

O BBBC039 tem máscaras anotadas. Reporta IoU e F1 para o backend clássico e para o Cellpose. Sem isto, ter dois backends é uma escolha sem dados; com isto, é uma comparação.

### 13.6 Pirâmide

| Nível | Ferramenta | Corre em |
|---|---|---|
| Propriedades e unitários | pytest, hypothesis | Cada push |
| Integração | testcontainers | Cada push |
| Lote dourado | pytest + fixtures versionadas | Cada push |
| Exatidão vs ground truth | pytest, marcado `slow` | Nightly |
| Carga | locust | Nightly |
| Caos | pumba + script | Nightly |

---

## 14. Observabilidade

### 14.1 Métricas

```
# API
http_request_duration_seconds{route,method,status}   histograma
ingestion_rejected_total{reason="queue_saturated"}   contador

# Fila
celery_queue_depth{queue}                            gauge
celery_task_duration_seconds{task,status}            histograma
celery_task_retries_total{task,error_class}          contador
celery_dlq_total                                     contador

# Worker  <- as que provam o Objetivo B
worker_memory_rss_bytes{worker_id}                   gauge
field_bytes_processed{size_bucket}                   histograma
chunks_processed_total                               contador
cells_detected_total                                 contador

# Inferência
inference_batch_size                                 histograma
inference_wait_time_seconds                          histograma

# Domínio
plate_z_prime{plate}                                 gauge
qc_rejected_total{reason}                            contador
```

### 14.2 Painéis

1. **Saúde do lote** — progresso em campos, throughput, taxa de erro, ETA.
2. **Memória vs. tamanho de input** — o painel âncora. `worker_memory_rss_bytes` contra `field_bytes_processed`. Uma linha plana enquanto a outra sobe é a prova visual do Objetivo B inteiro.
3. **Fila e backpressure** — profundidade, tempo de espera, rejeições `429`, workers ativos.
4. **Eficiência do batching** — distribuição do tamanho de batch. Concentrada em 1 significa que o batching não serve de nada, e deves saber isso.
5. **Qualidade científica** — Z′ por placa, taxa de rejeição de QC, contagem de células por poço. Um Z′ a cair ao longo do tempo é um problema de ensaio, não de software — e o painel deve tornar isso visível.

### 14.3 Carga

Locust com dois perfis: **rajada** (manifesto de 10.000 campos de uma vez, o cenário do microscópio) e **contínuo**. Corre com `--scale worker=1,2,4,8` e desenha a curva. O joelho onde o Postgres ou o I/O passam a ser o limite é resultado, não falha — encontra-o e documenta-o.

---

## 15. docker-compose.yml

Segredos em `.env`, fora do ficheiro. `cp .env.example .env` faz parte do arranque.

```yaml
x-app-env: &app-env
  DATABASE_URL: postgresql+psycopg://${PG_USER}:${PG_PASSWORD}@postgres:5432/${PG_DB}
  REDIS_URL: redis://redis:6379/0
  S3_ENDPOINT: http://minio:9000
  S3_ACCESS_KEY: ${MINIO_USER}
  S3_SECRET_KEY: ${MINIO_PASSWORD}
  S3_BUCKET_RAW: raw
  MLFLOW_TRACKING_URI: http://mlflow:5000
  QUEUE_HIGH_WATERMARK: "50000"
  QUEUE_LOW_WATERMARK: "10000"

services:
  traefik:
    image: traefik:v3.1
    command:
      - --providers.docker=true
      - --providers.docker.exposedbydefault=false
      - --entrypoints.web.address=:80
    ports: ["8080:80"]
    volumes: ["/var/run/docker.sock:/var/run/docker.sock:ro"]

  api:
    build: { context: ., dockerfile: docker/api.Dockerfile }
    environment: *app-env
    user: "10001:10001"
    read_only: true
    deploy: { replicas: 2 }
    labels:
      - traefik.enable=true
      - traefik.http.routers.api.rule=PathPrefix(`/v1`)
      - traefik.http.services.api.loadbalancer.server.port=8000
      - traefik.http.services.api.loadbalancer.healthcheck.path=/readyz
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }
      minio:    { condition: service_healthy }

  worker:
    build: { context: ., dockerfile: docker/worker.Dockerfile }
    command: celery -A app.tasks worker -Q default,dlq -l info --concurrency=2
    environment: *app-env
    user: "10001:10001"
    deploy:
      replicas: 4
      resources:
        limits: { memory: 2G }        # o limite É o teste do Objetivo B
    depends_on:
      redis: { condition: service_healthy }

  dispatcher:
    build: { context: ., dockerfile: docker/worker.Dockerfile }
    command: python -m app.dispatcher      # enfileira por blocos, respeita marcas
    environment: *app-env
    depends_on:
      redis: { condition: service_healthy }

  inference:
    image: nvcr.io/nvidia/tritonserver:24.08-py3
    command: tritonserver --model-repository=/models
    volumes: ["./models:/models:ro"]
    # deploy:
    #   resources:
    #     reservations:
    #       devices: [{ driver: nvidia, count: 1, capabilities: [gpu] }]

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: ${PG_DB}
    volumes: ["pgdata:/var/lib/postgresql/data"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER}"]
      interval: 5s
      retries: 10

  redis:
    image: redis:7-alpine
    command: >
      redis-server --appendonly yes --maxmemory 512mb
      --maxmemory-policy noeviction
    volumes: ["redisdata:/data"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 10

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_PASSWORD}
    ports: ["9001:9001"]
    volumes: ["miniodata:/data"]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 10

  minio-init:
    image: minio/mc
    depends_on:
      minio: { condition: service_healthy }
    entrypoint: >
      sh -c "mc alias set local http://minio:9000 $$MINIO_USER $$MINIO_PASSWORD &&
             mc mb -p local/raw && mc mb -p local/results"
    environment:
      MINIO_USER: ${MINIO_USER}
      MINIO_PASSWORD: ${MINIO_PASSWORD}

  mlflow:
    image: ghcr.io/mlflow/mlflow:latest
    command: >
      mlflow server --host 0.0.0.0
      --backend-store-uri postgresql://${PG_USER}:${PG_PASSWORD}@postgres:5432/mlflow
      --artifacts-destination s3://results
    ports: ["5000:5000"]
    depends_on:
      postgres: { condition: service_healthy }

  dashboard:
    build: { context: ., dockerfile: docker/dashboard.Dockerfile }
    command: streamlit run app/dashboard/main.py --server.port 8501
    environment: *app-env
    ports: ["8501:8501"]

  flower:
    image: mher/flower
    command: celery --broker=redis://redis:6379/0 flower
    ports: ["5555:5555"]

  prometheus:
    image: prom/prometheus
    volumes: ["./ops/prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana
    environment: { GF_AUTH_ANONYMOUS_ENABLED: "true" }
    volumes: ["./ops/grafana:/etc/grafana/provisioning:ro"]
    ports: ["3000:3000"]

volumes: { pgdata: {}, redisdata: {}, miniodata: {} }
```

O `limits: memory: 2G` no worker não é decoração — é o mecanismo que faz o teste falhar ruidosamente (OOMKill, exit 137) se a leitura chunked não estiver correta. Um teste que não pode falhar não prova nada.

---

## 16. Experiência de desenvolvimento e CI

### Makefile

Se não arrancar em cinco minutos, ninguém o corre — incluindo tu, daqui a três meses.

```makefile
up:      ## Levanta o stack e corre migrações
	cp -n .env.example .env || true
	docker compose up -d --build
	docker compose run --rm api alembic upgrade head

seed:    ## Descarrega uma semana do BBBC021 e submete um lote
	python scripts/fetch_bbbc021.py --weeks 1
	python scripts/submit_batch.py --plate 8291

bench:   ## Matriz de batching, resultados para o MLflow
	python scripts/bench_batching.py --workers 1,4,8 --backends none,manual,triton

chaos:   ## Mata workers a meio e verifica as invariantes
	python scripts/chaos_test.py --fields 500 --kill 2

golden:  ## Reprocessa o lote dourado e compara com a referência
	pytest tests/golden -v

down:
	docker compose down -v
```

### GitHub Actions

```yaml
on: [push, pull_request]
jobs:
  quality:
    steps:
      - run: ruff check . && ruff format --check .
      - run: mypy app/
      - run: trivy fs --severity HIGH,CRITICAL .
  test:
    steps:
      - run: pytest tests/unit tests/property -v --cov=app
      - run: pytest tests/integration -v      # testcontainers
      - run: pytest tests/golden -v
  nightly:
    if: github.event_name == 'schedule'
    steps:
      - run: pytest tests/accuracy -v         # BBBC039, IoU e F1
      - run: make chaos
      - run: locust --headless -u 100 -r 10 -t 5m
```

---

## 17. Semântica de falhas

### 17.1 Configuração Celery

```python
task_acks_late = True                 # ack só DEPOIS de terminar
task_reject_on_worker_lost = True     # worker morto -> tarefa volta à fila
worker_prefetch_multiplier = 1        # não acumular trabalho num worker
task_time_limit = 1800                # hard kill
task_soft_time_limit = 1500           # exceção capturável, permite limpeza
broker_transport_options = {"visibility_timeout": 3600}
```

`visibility_timeout` **tem de exceder** a tarefa mais longa. Se não exceder, o Redis reentrega uma tarefa ainda a correr e passas a ter dois workers no mesmo campo — a produzir duplicados que a constraint apanha, mas depois de desperdiçar o dobro da computação.

### 17.2 Idempotência

`process_field(field_id, run_id)` — ambos estáveis, logo reexecutar é seguro desde que a escrita seja idempotente. Dentro de uma transação: apagar as linhas de `(field_id, run_id)` e reinserir. Assim uma reexecução que produza *menos* células não deixa órfãs da tentativa anterior.

```sql
DELETE FROM cell_metrics WHERE field_id = %s AND run_id = %s;
INSERT INTO cell_metrics (field_id, run_id, label, area, features)
VALUES (...)
ON CONFLICT (field_id, run_id, label) DO UPDATE
SET area = EXCLUDED.area, features = EXCLUDED.features;
```

### 17.3 Retries tipados

```python
@app.task(bind=True, max_retries=3,
          autoretry_for=(IOError, S3Error, ConnectionError),
          retry_backoff=True, retry_backoff_max=300, retry_jitter=True)
def process_field(self, field_id, run_id):
    ...
```

- **Transitória** (rede, S3 indisponível, timeout) → retry com backoff exponencial e jitter.
- **Permanente** (ficheiro corrompido, canal em falta, formato inválido) → sem retry. Direto para `FAILED` com `error_class='PERMANENT'`, e a mensagem para a fila `dlq`.

Repetir três vezes uma falha permanente é desperdício puro. A distinção é a parte que interessa aprender, e é verificável: `celery_task_retries_total{error_class="PERMANENT"}` tem de ser zero.

### 17.4 Reconciliação

Um job periódico procura campos em `PROCESSING` há mais tempo que `visibility_timeout` e reenfileira-os. É a rede de segurança que torna aceitável o Redis como broker (ADR-001) — o Postgres é a fonte de verdade, a fila é só transporte.

O lote termina em `PARTIAL` se alguns campos falharem. Falhar 10.000 campos por causa de 3 ficheiros corrompidos seria absurdo.

---

## 18. Decisões arquiteturais

### ADR-001 — Redis como broker, com reconciliação

Simples, já no stack, serve também de backend de resultados. **Custo:** sem persistência forte; um crash entre enfileirar e o ack perde tarefas. **Mitigação:** Postgres é a fonte de verdade; o job de reconciliação (§17.4) reenfileira o que ficou preso. **Reconsiderar se:** a perda for observável nos testes → RabbitMQ com filas duráveis e confirmações de publicação. A reconciliação vale a pena de qualquer forma — é a lição mais transferível deste ADR.

### ADR-002 — Ingestão por referência

Passar 10.000 ficheiros de centenas de MB pela API são vários TB pelo cabo, e a promessa de "aceite em segundos" seria falsa. **Custo:** o cliente precisa de escrita no MinIO. **Mitigação:** URLs pré-assinados com escopo e validade limitados ao lote.

### ADR-003 — O campo como unidade de processamento

Os canais de um campo têm de ser lidos juntos: segmenta-se num, mede-se nos outros. **Custo:** um campo com um canal em falta é rejeitado inteiro. **Aceite:** processar parcialmente produziria medições irreconciliáveis, que é pior do que não produzir nada.

### ADR-004 — Bibliotecas por omissão, manual como experiência

Invertida face à v2. Escrever menos código é escrever menos bugs, e o tempo vai para os objetivos que interessam. Único componente manual: o micro-batching, porque o mecanismo é o objetivo — e mesmo esse fica atrás da mesma interface do Triton, com benchmark obrigatório. **Sem o benchmark, a exceção é injustificável.**

### ADR-005 — `run_id` em vez de `algorithm_version`

Uma string de versão não captura o contexto: mesma versão, config diferente, resultado diferente. O `run` liga o resultado ao digest do contentor, à config, à seed e ao lockfile. **Custo:** mais um join. **Ganho:** a pergunta "como é que isto foi produzido?" tem resposta.

### ADR-006 — `features` em JSONB

O conjunto de features difere entre backends e muda com as experiências. Colunas rígidas obrigariam a uma migração por experiência. **Custo:** consultas mais verbosas, sem índice por feature. **Mitigação:** promover a coluna qualquer feature que se torne central.

### ADR-007 — Sem alta disponibilidade

Ver §1.2. Sem orquestrador com failover, "HA" seria alegação falsa. O sistema escala horizontalmente e tolera perda de workers — é isso que se afirma e nada mais.

---

## 19. Critérios de aceitação

| Obj. | Critério | Como medir |
|---|---|---|
| A | p95 do POST de manifesto < 200 ms com 10.000 campos | Locust, perfil rajada |
| A | Nenhum pedido HTTP bloqueia > 1 s durante processamento ativo | Locust em paralelo |
| A | Submeter 10× a capacidade → `429`, fila abaixo do limite, 0 campos perdidos | Teste de backpressure |
| B | RSS do worker ≤ 1.5 GB constante, de 100 MB a 8 GB por campo | Grafana, painel 2 |
| B | Zero OOMKills com `memory: 2G` na suite de ficheiros grandes | exit code 137 |
| C | Zero duplicados em `(field_id, run_id, label)` após caos | Query de invariantes |
| C | Zero campos presos em `PROCESSING` 5 min após o fim | Job de reconciliação |
| C | `celery_task_retries_total{error_class="PERMANENT"} == 0` | Prometheus |
| D | Speedup ≥ 3× com batching vs. sem, 4 workers | Tabela §11 |
| D | Escalabilidade ≥ 3.2× de 1 para 4 workers | Curva do Locust |
| E | Painéis respondem "onde está o estrangulamento?" sem entrar em contentores | Revisão qualitativa |
| F | Z′ > 0.5 nas placas de controlo do BBBC021 | `plate_qc` |
| F | IoU ≥ 0.8 vs. ground truth do BBBC039 | `pytest tests/accuracy` |
| F | MoA em not-same-compound claramente acima da linha de base do acaso | Matriz de confusão |
| G | Reprocessar com o mesmo `image_digest` e `config_hash` dá resultado idêntico (caminho clássico) | `make golden` |
| G | Divergência do caminho GPU medida e documentada | Relatório de tolerância |
| H | Propriedades do tiling passam para qualquer dimensão gerada | `hypothesis` |
| H | Contagem de células invariante ao tamanho de chunk (±1%) | Teste de propriedade |

---

## 20. Roadmap

**Fase 1 — esqueleto.** Modelo de dados com `fields` e `runs`, Alembic, FastAPI com manifesto e `202`, Celery com tarefa trivial, compose, Makefile. *Meta: um campo entra por um lado e sai marcado do outro.*

**Fase 2 — memória.** `bioio` + `dask.map_overlap`, política de labels por centróide, pipeline `scikit-image`, escrita de `cell_metrics`, limite de 2 GB, suite de ficheiros grandes. *Meta: o gráfico de RSS plano.*

**Fase 3 — testes.** Propriedades do tiling, testcontainers, lote dourado, CI a correr tudo. *Meta: a suite falha quando deve falhar.*

**Fase 4 — telemetria e backpressure.** Prometheus, painéis, despachante por blocos, `429`, Locust. *Meta: o painel 2 é convincente e a fila nunca passa do limite.*

**Fase 5 — falhas.** Retries tipados, DLQ, reconciliação, teste de caos com pumba. *Meta: matar workers e as invariantes aguentam.*

**Fase 6 — ciência.** BBBC021, `well_layout`, QC de foco, `pycytominer`, Z′, IC50, MoA, dashboard com heatmap e curvas. *Meta: um biólogo reconhece o que está a ver.*

**Fase 7 — GPU.** Cellpose ou StarDist no Triton, benchmark do batching, exatidão contra o BBBC039. *Meta: a tabela do §11 preenchida.*

**As fases 1 a 5 já constituem um projeto defensável.** A 6 é o que o torna credível para alguém do domínio. A 7 é o que o torna incomum.

---

## 21. Dados

| Conjunto | Uso | Notas |
|---|---|---|
| **BBBC021** | Principal | 13.200 imagens, MCF-7, 113 compostos, 3 canais, rótulos de MoA para 103 combinações composto-concentração. Zips semanais. `pybbbc` dá interface Python e pré-processamento |
| **BBBC039** | Ground truth de segmentação | Núcleos de U2OS com máscaras anotadas — critério F |
| **BBBC005** | Testes sintéticos | Células simuladas com desfoque controlado; calibra o limiar de QC de foco |
| Sintéticos grandes | Testes de memória | TIFFs de 1–8 GB gerados com `tifffile` para exercitar o Objetivo B sem descarregar terabytes |
| Lote dourado | Regressão | 20 campos fixos, versionados, com output de referência |

---

## 22. Documentação

Um README que abre com **o gráfico de memória plana** e a **matriz de confusão de MoA**. São os dois resultados; tudo o resto é como lá chegaste.

E uma secção de **resultados negativos**: o bug do `visibility_timeout`, o batching que não compensou com um worker, a leitura chunked que não funcionou à primeira, a feature que parecia discriminativa e era artefacto de segmentação. Um README que documenta o que correu mal é mais credível do que um com dez selos verdes — e é a parte que quem te entrevista vai querer discutir.

---

## Apêndice — armadilhas conhecidas

1. **`visibility_timeout` menor que a tarefa mais longa** → reentrega de tarefas ainda a correr. O bug mais comum deste tipo de sistema.
2. **`map_overlap` com `depth` menor que a maior célula** → objetos cortados nas fronteiras. `depth` > diâmetro máximo esperado, sempre.
3. **Esperar que o `map_overlap` reconcilie labels de instância** → não reconcilia. Rótulos são por chunk; a política de centróide é tua.
4. **Calcular o SHA-256 depois do upload** → obriga a reler o ficheiro. Calcula em streaming, no mesmo passo da escrita.
5. **`worker_prefetch_multiplier` no valor por omissão (4)** → um worker reserva 4 campos grandes e os outros ficam ociosos com a fila cheia. Põe a 1.
6. **Agregar por poço antes de todos os campos terminarem** → estatísticas sobre dados parciais. Contador transacional ou chord do Celery.
7. **Normalizar contra controlos de outra placa** → efeitos de lote destroem a comparação. Sempre intra-placa.
8. **Não excluir o próprio composto na avaliação de MoA** → estás a medir reprodutibilidade técnica, não generalização biológica. Exatidão inflacionada e sem sentido.
9. **IC50 extrapolado para fora da gama testada** → a forma mais comum de produzir números bonitos e falsos. Exige `ic50` dentro da gama e `r_squared` acima do limiar.
10. **Ler o TIFF com PIL** → carrega tudo em memória e anula o Objetivo B inteiro. `bioio`/`tifffile`/`zarr`/`openslide`, sempre.
11. **Tag de imagem em vez de digest na linhagem** → tags são mutáveis; a reprodutibilidade fica por um fio.
