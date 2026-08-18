# Spike de memória — Objetivo B (§21.3)

Gerado em 2026-08-18T11:03:29+00:00 por `scripts/spike_memory.py`.
Carril HCS, `overlap=32`, canal de segmentação
`DAPI`, 6 campos por processo.

Linha de base, só os imports e nenhum trabalho: **186 MiB**.


| chunk | procs | pico (MiB) | acima da base | estac. (MiB) | agregado (MiB) | campos/s | B/linha |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 243 | 57 | 233 | 243 | 1.9 | 142 |
| 256 | 2 | 237 | 51 | 234 | 472 | 3.6 | 145 |
| 256 | 4 | 238 | 52 | 234 | 946 | 2.8 | 145 |
| 512 | 1 | 252 | 67 | 246 | 252 | 1.8 | 142 |
| 512 | 2 | 252 | 67 | 235 | 504 | 2.3 | 145 |
| 512 | 4 | 260 | 74 | 244 | 1025 | 2.5 | 145 |
| 1024 | 1 | 300 | 114 | 287 | 300 | 1.2 | 142 |
| 1024 | 2 | 300 | 115 | 290 | 596 | 1.7 | 145 |
| 1024 | 4 | 298 | 112 | 277 | 1182 | 2.0 | 145 |
| 4096 | 1 | 307 | 121 | 282 | 307 | 1.0 | 142 |
| 4096 | 2 | 321 | 135 | 293 | 623 | 1.5 | 145 |
| 4096 | 4 | 313 | 127 | 292 | 1219 | 2.1 | 145 |

## Como ler isto

**A linha de base é a maior parte do número.** O scipy, o scikit-image, o pandas e o
botocore custam 186 MiB no momento em que são importados, e esse custo paga-se uma
vez por worker seja qual for o chunk. A coluna *acima da base* é a única que o chunking
decide; o resto é o preço de entrada.

**Pico por processo** é o que decide o limite de memória de um worker. **Pico agregado** é
a soma no pior instante e é o que um limite de contentor veria se lá corressem vários
processos — não é a soma dos picos individuais, porque os picos não coincidem.

**Estado estacionário** é a mediana depois do arranque. A diferença para o pico é o custo
de inicialização, e é aí que o consumo mau se esconde: é por isso que os dois aparecem
separados em vez de numa média que os fundiria.

## Recomendação

**`CHUNK_SIZE=256`** — a 4 processos, dentro de 15% do melhor débito (2.8 campos/s), o menor pico é 238 MiB.

Não é o chunk mais rápido. A memória acima da base é um sinal limpo e monótono; o débito
não é, e move-se mais de dez por cento entre corridas da mesma configuração. Uma regra que
perseguisse o número mais rápido recomendaria um chunk diferente de cada vez.

O chunk **não** muda a contagem de células. Com o limiar de Otsu calculado uma vez por
imagem (§8.1), a contagem é exatamente invariante — 0,0% de amplitude em campos reais a
256, 512, 1024 e bloco único. Escolher aqui é escolher memória e débito, não resultado.

Continua no `config_hash` (§12), na mesma: identifica a corrida e o custo a que correu.

## Bytes por linha e o ADR-007

Mediana de **145 bytes por linha** de Parquet. A 13200 campos por
~1000 células, uma corrida completa dá cerca de **1.8 GiB**. É o
número em que o ADR-007 se decide, e é a razão de `cell_metrics` não viver em Postgres.

## O controlo negativo

Por fazer. Pertence ao carril WSI — carregar uma lâmina inteira com PIL sob o limite de
1,5 GB e registar o OOMKill — e exige lâminas em disco, que ainda não estão. Sem ele, esta
medição mostra que o consumo é baixo mas não prova que a montagem seria capaz de o detetar
se fosse alto.
