# Exatidão contra ground truth — BBBC039 (§13.5)

Gerado em 2026-08-18T22:03:28+00:00 por `scripts/accuracy_bbbc039.py`.
Split **test** (50 campos), backend clássico, campo inteiro num bloco.

**F1 médio em IoU 0,50:0,95 = 0.767**
· F1@0,50 = 0.897
· F1@0,75 = 0.838

Núcleos: **4632 previstos** contra **4727 anotados** (98.0%).

| IoU | TP | FP | FN | precisão | recall | F1 | IoU médio dos pares |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.50 | 4199 | 433 | 528 | 0.907 | 0.888 | **0.897** | 0.900 |
| 0.55 | 4142 | 490 | 585 | 0.894 | 0.876 | **0.885** | 0.905 |
| 0.60 | 4107 | 525 | 620 | 0.887 | 0.869 | **0.878** | 0.908 |
| 0.65 | 4065 | 567 | 662 | 0.878 | 0.860 | **0.869** | 0.911 |
| 0.70 | 4004 | 628 | 723 | 0.864 | 0.847 | **0.856** | 0.915 |
| 0.75 | 3922 | 710 | 805 | 0.847 | 0.830 | **0.838** | 0.919 |
| 0.80 | 3764 | 868 | 963 | 0.813 | 0.796 | **0.804** | 0.925 |
| 0.85 | 3492 | 1140 | 1235 | 0.754 | 0.739 | **0.746** | 0.933 |
| 0.90 | 2862 | 1770 | 1865 | 0.618 | 0.605 | **0.612** | 0.946 |
| 0.95 | 1343 | 3289 | 3384 | 0.290 | 0.284 | **0.287** | 0.969 |

## Como ler isto

**Ao nível do objeto, não do pixel.** Uma máscara binária pontuada pixel a pixel não
distingue um método que funde dois núcleos encostados de um que os separa — ambos pintam os
mesmos pixels. Separá-los é o problema mais difícil deste pipeline, é para isso que existe o
watershed, e é isso que o `min_distance` afina. A métrica tem de o conseguir ver.

**O limiar importa.** O F1@0,50 diz "encontrou o objeto". O F1@0,75 diz "encontrou-o e o
contorno está aproximadamente certo". Citar só o primeiro favorece um método que acerta nos
sítios e erra nas fronteiras, que é exatamente a falha de um watershed sobre um limiar mau.
A média sobre a varredura é a convenção do COCO e é o número que vale a pena citar.

**Contagens agregadas, não médias por imagem.** Uma média de F1 por imagem deixa um campo
com três núcleos pesar tanto como um com trezentos, e transforma a métrica numa afirmação
sobre a composição do conjunto.

## O que estes números dizem

**Prevemos 4632 núcleos onde há 4727 — 98.0%.** O pipeline
sobre-segmenta. Com recall de 0.888 a IoU 0,50 encontra quase tudo o que lá
está; com precisão de 0.907, cerca de um quinto do que reporta não
corresponde a nenhum núcleo anotado. O suspeito é o `min_distance`, que semeia o watershed:
foi a constante que o teste de mutação do lote dourado mostrou ser a mais sensível, e
semear duas vezes dentro de um núcleo parte-o em dois.

**Afinado uma vez, no split de validação.** O `min_distance` passou de 10 para 20, escolhido
sobre o **validation** e medido aqui sobre o **test** uma única vez (`docs/tuning-bbbc039.md`).
Antes: F1 médio 0,698, F1@0,50 0,845, 110,2% dos núcleos anotados. A sobre-segmentação
desapareceu praticamente.

A validação previu +0,080 de F1 médio e o test entregou +0,069. Essa proximidade é o que
distingue afinar de sobreajustar: se o ganho tivesse evaporado no test, a escolha estaria a
descrever o ruído do validation.

## O que a ground truth precisou de descodificação

As máscaras do BBBC039 não trazem um inteiro por núcleo: dizem **quantos núcleos cobrem cada
pixel** — 0 fundo, 1 um núcleo, 2 e 3 onde vizinhos se sobrepõem. As instâncias são as
componentes ligadas dos 1, crescidas para os pixels disputados por watershed.

Ler o ficheiro ingenuamente dá **três objetos por imagem em vez de cem**, e as 200 máscaras
somariam 531 núcleos em vez de 19 391. Teria produzido uma pontuação bonita e sem sentido —
e nada mais neste repositório teria dado por isso.
