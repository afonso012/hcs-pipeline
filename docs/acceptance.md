# Critérios de aceitação (§19) — estado medido

Não é uma lista do que se espera. É o que foi medido, com o número, e onde está a medição.
As linhas que falham estão marcadas como tal, porque uma tabela onde tudo passa é uma tabela
que ninguém tentou reprovar.

| Obj. | Critério | Estado | Medido |
|---|---|---|---|
| A | p95 do POST de manifesto < 200 ms com 10.000 unidades | ❌ **falha** | 2,1 ms/unidade, linear: 512 ms a 250 unidades, 10,5 s a 5000. Dez mil ≈ 21 s |
| A | Nenhum pedido HTTP bloqueia > 1 s durante processamento ativo | ❌ **falha** | Mesma causa: o manifesto é síncrono, 6 linhas por unidade em 4 tabelas |
| A | Submeter 10× a capacidade → `429`, fila abaixo do limite, 0 unidades perdidas | ✅ passa | `tests/test_backpressure.py`; 429 com `Retry-After`, nenhum run criado, unidades intactas |
| B-WSI | RSS ≤ 1,5 GB constante, lâminas de 0,2 a 4 GB | ✅ passa | 256× mais pixels, pico em 1,01×; 4,2 Mpx → 1074 Mpx a ~800 MiB. `docs/spike-memory.md` |
| B-WSI | Controlo negativo: leitura ingénua com PIL sob 1,5 GB produz OOM registado | ✅ passa | Saída 137, `OOMKilled: sim`, 6,1 s. `docs/negative-control.md` |
| B-HCS | RSS agregado cresce linearmente com o nº de workers | ✅ passa | 243 → 509 → 1020 MiB a 1, 2 e 4 processos. `docs/spike-memory.md` |
| B-HCS | RSS por worker independente da profundidade da fila | ✅ passa | 320 MiB por processo com a fila a 474 e a 0 |
| C | Zero duplicados em `(work_unit_id, run_id, label)` após caos | ✅ passa | 0 duplicados após SIGKILL. `docs/chaos.md` |
| C | Zero unidades presas em `PROCESSING` 5 min após o fim | ✅ passa | 0 presas, 480/480, run `COMPLETED`, com paciência de 300 s. `docs/chaos.md` |
| C | `celery_task_retries_total{error_class="PERMANENT"} == 0` | ✅ passa | 0 falhas permanentes com `attempts > 1` |
| D-workers | Escalabilidade ≥ 3,2× de 1 para 4 workers | ⚠️ **por medir** | O Locust existe; falta correr `--scale worker=1,2,4` |
| E | Painéis respondem "onde está o estrangulamento?" sem entrar em contentores | ⚠️ parcial | Painéis 1–4 com dados; 5 e 6 vazios até às Fases 7 e 6 |
| F | IoU e F1 medidos para **ambos** os backends no BBBC039 | ⚠️ parcial | Clássico: F1@0,50 = 0,845, média 0,50:0,95 = 0,698. Cellpose é Fase 7 |
| G | Reprocessar com a mesma linhagem dá resultado idêntico | ✅ passa | `make golden`: 3556 células em 56 campos, 8 doses dentro de 2% |
| H | Propriedades do tiling passam para qualquer dimensão gerada | ✅ passa | `hypothesis`, `tests/test_geometry.py` |
| H | Contagem de células invariante ao chunk (±1%) | ✅ passa | **0,0%** em campos reais, a 256/512/1024/bloco único |
| H | `control_type` nunca atribuído por composto | ✅ passa | `tests/test_controls.py`; taxol POSITIVO só a 0,3 µM de oito |

Os critérios de Fase 6 e 7 — Z′, efeito de lote, IC50 com `r_squared > 0,8`, MoA — não
aparecem porque as fases não existem. Marcá-los como pendentes seria repetir o roteiro.

## As três falhas

**A ingestão é síncrona e não chega aos 200 ms.** Cada unidade são seis linhas em quatro
tabelas, a ~2,1 ms. Não é a máquina, é o desenho. Saídas: ingestão assíncrona, `COPY` em vez
de `INSERT`, ou corrigir o critério para um número medido. As duas primeiras são trabalho, a
terceira é uma decisão sobre o que o projeto promete.

**~~Uma unidade fica presa em `PROCESSING`~~ — resolvido.** A varredura do §16.4 existe e o
teste de caos passa as quatro invariantes com 300 s de paciência. O caminho até lá corrigiu
uma contradição na especificação: o §16.4 manda esperar o `visibility_timeout`, que é uma
hora, e o §22 pede zero unidades presas cinco minutos depois — nenhum temporizador satisfaz
os dois. A varredura pergunta agora aos workers o que estão a executar, e reclama o que
ninguém tem em mãos.

**A escalabilidade não foi medida.** O perfil do Locust está escrito e nunca correu contra
`--scale worker=1,2,4`. É a única linha aqui que falha por não ter sido tentada, e por isso
é a mais barata de resolver.

## Uma nota sobre o que os critérios não dizem

O §19 evita deliberadamente alvos como "Z′ > 0,5" ou "IoU ≥ 0,8". Esses testariam a sorte do
conjunto de dados e não o sistema. O F1 de 0,845 do backend clássico não é um portão: é o
achado que motiva ter um segundo backend, e transformá-lo em portão convidaria a afinar o
clássico contra o conjunto de teste até passar — que é exatamente o que o §13.2 proíbe pelo
nome.
