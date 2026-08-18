# Spike de memória — Objetivo B (§21.3)

Gerado em 2026-08-18T15:15:37+00:00 por `scripts/spike_memory.py`.
Carril HCS, `overlap=32`, canal de segmentação
`DAPI`, 6 campos por processo.

Linha de base, só os imports e nenhum trabalho: **179 MiB**.


| chunk | procs | pico (MiB) | acima da base | estac. (MiB) | agregado (MiB) | campos/s | B/linha |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 243 | 65 | 235 | 243 | 1.5 | 142 |
| 256 | 2 | 235 | 57 | 210 | 469 | 1.4 | 145 |
| 256 | 4 | 235 | 57 | 205 | 905 | 1.5 | 145 |
| 512 | 1 | 250 | 72 | 236 | 250 | 0.8 | 142 |
| 512 | 2 | 252 | 73 | 227 | 494 | 1.1 | 145 |
| 512 | 4 | 260 | 82 | 222 | 1019 | 1.6 | 145 |
| 1024 | 1 | 291 | 112 | 289 | 291 | 1.2 | 142 |
| 1024 | 2 | 296 | 117 | 272 | 591 | 1.4 | 145 |
| 1024 | 4 | 298 | 119 | 281 | 1173 | 1.8 | 145 |
| 4096 | 1 | 308 | 130 | 267 | 308 | 1.0 | 142 |
| 4096 | 2 | 305 | 126 | 265 | 606 | 1.2 | 145 |
| 4096 | 4 | 310 | 131 | 288 | 1229 | 1.6 | 145 |

## Como ler isto

**A linha de base é a maior parte do número.** O scipy, o scikit-image, o pandas e o
botocore custam 179 MiB no momento em que são importados, e esse custo paga-se uma
vez por worker seja qual for o chunk. A coluna *acima da base* é a única que o chunking
decide; o resto é o preço de entrada.

**Pico por processo** é o que decide o limite de memória de um worker. **Pico agregado** é
a soma no pior instante e é o que um limite de contentor veria se lá corressem vários
processos — não é a soma dos picos individuais, porque os picos não coincidem.

**Estado estacionário** é a mediana depois do arranque. A diferença para o pico é o custo
de inicialização, e é aí que o consumo mau se esconde: é por isso que os dois aparecem
separados em vez de numa média que os fundiria.


## Carril WSI — a área cresce, a memória não

Regiões quadradas do nível 0 da `CMU-1`, chunk fixo, cada uma em processo limpo.

| área (Mpx) | núcleos | segundos | pico (MiB) | acima da base |
|---:|---:|---:|---:|---:|
| 4.2 | 2 | 1.4 | 789 | 611 |
| 16.8 | 14 | 4.6 | 920 | 741 |
| 67.1 | 57 | 15.2 | 880 | 702 |
| 268.4 | 9611 | 59.6 | 891 | 712 |
| 1073.7 | 170149 | 326.8 | 895 | 716 |

**256x mais pixels, e o pico fica em 1.13x.** É o gráfico do §11 painel 2 —
uma linha plana enquanto a outra sobe — dito em números em vez de desenhado.

A prova de que é planura e não sorte está na coluna não ser monótona: a região mais cara
não é a maior, é a de 16.8 Mpx. Se a memória seguisse a área, o maior
seria sempre o pior. A dispersão que resta é ruído da máquina, e é maior do que o efeito
da área porque o efeito da área é nenhum.

O pico é mais alto do que no carril HCS porque o leitor é outro: o OpenSlide e a
deconvolução de cor custam mais a importar do que o tifffile. Isso é a base, não o
crescimento.

## Recomendação

**`CHUNK_SIZE=256`** — a 4 processos, dentro de 15% do melhor débito (1.8 campos/s), o menor pico é 235 MiB.

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
