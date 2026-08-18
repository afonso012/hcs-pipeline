# Spike de memória — Objetivo B (§21.3)

Gerado em 2026-08-18T13:08:38+00:00 por `scripts/spike_memory.py`.
Carril HCS, `overlap=32`, canal de segmentação
`DAPI`, 6 campos por processo.

Linha de base, só os imports e nenhum trabalho: **185 MiB**.


| chunk | procs | pico (MiB) | acima da base | estac. (MiB) | agregado (MiB) | campos/s | B/linha |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 234 | 49 | 212 | 234 | 1.8 | 142 |
| 256 | 2 | 237 | 52 | 234 | 473 | 3.6 | 145 |
| 256 | 4 | 238 | 53 | 209 | 927 | 4.3 | 145 |
| 512 | 1 | 251 | 66 | 239 | 251 | 2.2 | 142 |
| 512 | 2 | 266 | 80 | 261 | 528 | 4.0 | 145 |
| 512 | 4 | 251 | 66 | 242 | 992 | 4.9 | 145 |
| 1024 | 1 | 300 | 115 | 292 | 300 | 2.3 | 142 |
| 1024 | 2 | 305 | 119 | 294 | 605 | 4.2 | 145 |
| 1024 | 4 | 296 | 111 | 273 | 1172 | 4.8 | 145 |
| 4096 | 1 | 313 | 128 | 311 | 313 | 2.3 | 142 |
| 4096 | 2 | 312 | 127 | 308 | 622 | 3.9 | 145 |
| 4096 | 4 | 308 | 123 | 275 | 1217 | 4.4 | 145 |

## Como ler isto

**A linha de base é a maior parte do número.** O scipy, o scikit-image, o pandas e o
botocore custam 185 MiB no momento em que são importados, e esse custo paga-se uma
vez por worker seja qual for o chunk. A coluna *acima da base* é a única que o chunking
decide; o resto é o preço de entrada.

**Pico por processo** é o que decide o limite de memória de um worker. **Pico agregado** é
a soma no pior instante e é o que um limite de contentor veria se lá corressem vários
processos — não é a soma dos picos individuais, porque os picos não coincidem.

**Estado estacionário** é a mediana depois do arranque. A diferença para o pico é o custo
de inicialização, e é aí que o consumo mau se esconde: é por isso que os dois aparecem
separados em vez de numa média que os fundiria.

## Recomendação

**`CHUNK_SIZE=256`** — a 4 processos, dentro de 15% do melhor débito (4.9 campos/s), o menor pico é 238 MiB.

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

Feito, e vive em [negative-control.md](negative-control.md). O nível 0 da `CMU-1` carregado
inteiro sob 1536m morre com `OOMKilled` e saída 137 ao fim de 6 segundos; os mesmos pixels,
pelo caminho por blocos e sob o mesmo limite, dão 224 560 núcleos e concluem.

É esse par que faz a tabela acima significar alguma coisa. Sem ele, dizer que o consumo é
baixo não prova que a montagem seria capaz de o detetar se fosse alto.
