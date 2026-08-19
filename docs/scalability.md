# Escalabilidade — objetivo D-workers (§22)

Gerado em 2026-08-19T10:52:14+00:00 por `scripts/scalability.py`.
O mesmo lote de 480 unidades, uma vez por contagem de workers.

| workers | segundos | unidades/s | speedup | eficiência |
|---:|---:|---:|---:|---:|
| 1 | 243.3 | 1.97 | 1.00x | 100% |
| 2 | 239.7 | 2.00 | 1.01x | 51% |
| 4 | 350.4 | 1.37 | 0.69x | 17% |

**Não cumpre.** 0.69x de 1 para 4 workers, contra um alvo de 3.2x.

## Como ler isto

**Eficiência** é o speedup dividido pelo aumento de workers: 100% seria escalar
perfeitamente. O que cai abaixo disso é contenção — Postgres, MinIO, ou a máquina.

**Uma curva sub-linear é um resultado, não uma falha.** O §14.3 diz isso pelo nome: o joelho
onde o Postgres ou o I/O passam a ser o limite encontra-se e documenta-se. Quatro workers a
partilhar uma base de dados, um MinIO e um portátil dobram nalgum sítio, e saber onde vale
mais do que uma reta.

**Medido no processamento, não na ingestão.** O Locust mede a que velocidade a API aceita
manifestos; este critério é sobre a que velocidade a frota os processa. São sistemas
diferentes a partilhar uma base de dados, e ler um pelo outro é como um número deixa de
significar o que diz.
