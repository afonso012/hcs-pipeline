# hcs-pipeline

Pipeline distribuído para análise de imagem de *high-content screening*: recebe placas de
microscopia (e lâminas inteiras de vários GB), segmenta e mede as células em paralelo, e
guarda os resultados com linhagem completa para poderem ser reproduzidos.

> Projeto de aprendizagem, feito para pôr à prova engenharia de dados e sistemas
> distribuídos com dados científicos reais (BBBC021, BBBC039, OpenSlide). Não se destina a
> uso clínico.

## O que faz

- **API** (FastAPI) recebe um manifesto de placas e cria as unidades de trabalho, com
  *backpressure* (`429` + `Retry-After`) quando a fila está cheia.
- **Workers** (Celery + Redis) leem as imagens do armazenamento (MinIO/S3), segmentam os
  núcleos (Otsu + watershed) e medem cada célula, bloco a bloco, com memória constante.
- **PostgreSQL** guarda o estado de cada unidade por execução; as métricas por célula vão
  para Parquet.
- **Observabilidade** com Prometheus e Grafana.
- Tudo arranca com Docker Compose; CI no GitHub Actions corre lint, tipos, testes e builds.

## Resultados medidos

Os critérios de aceitação estão em [`docs/acceptance.md`](docs/acceptance.md), **incluindo
os que falham** — uma tabela onde tudo passa é uma tabela que ninguém tentou reprovar.

| Teste | Resultado | Detalhe |
|---|---|---|
| Exatidão contra anotação (BBBC039) | F1@0,50 = **0,897** | [`docs/accuracy-bbbc039.md`](docs/accuracy-bbbc039.md) |
| Memória com lâminas de 0,2 a 4 GB | ~800 MiB constante com 256× mais pixels | [`docs/spike-memory.md`](docs/spike-memory.md) |
| Caos: matar workers com SIGKILL a meio | 0 duplicados, 0 unidades presas, 480/480 | [`docs/chaos.md`](docs/chaos.md) |
| Reprodutibilidade (lote dourado) | resultado idêntico ao reprocessar | `make golden` |
| Escalabilidade de 1 para 4 workers | ❌ 0,69× (alvo 3,2×) — estrangulamento documentado | [`docs/scalability.md`](docs/scalability.md) |

## Arrancar

Requer Docker e Python 3.12 com [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
make up        # sobe a stack e corre as migrações
make smoke     # confirma que a stack funciona de ponta a ponta
make test      # testes (os de integração arrancam os próprios contentores)
make help      # todos os comandos
```

## Estrutura

```
hcs/          código: api, worker, imaging, science, db, storage
migrations/   esquema da base de dados (Alembic)
scripts/      seed de dados, testes de caos, memória, escalabilidade e exatidão
tests/        testes unitários, de integração e de propriedades (hypothesis)
docs/         medições e relatórios
docker/       Dockerfiles, Prometheus, Grafana, Traefik
```

A especificação técnica completa está em
[`especificacao-pipeline-hcs-v4.md`](especificacao-pipeline-hcs-v4.md).
