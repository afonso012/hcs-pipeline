# Exatidão contra ground truth — BBBC039 (§13.5)

Gerado em 2026-08-18T21:24:51+00:00 por `scripts/accuracy_bbbc039.py`.
Split **test** (50 campos), backend clássico, campo inteiro num bloco.

**F1 médio em IoU 0,50:0,95 = 0.698**
· F1@0,50 = 0.845
· F1@0,75 = 0.756

Núcleos: **5208 previstos** contra **4727 anotados** (110.2%).

| IoU | TP | FP | FN | precisão | recall | F1 | IoU médio dos pares |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.50 | 4197 | 1011 | 530 | 0.806 | 0.888 | **0.845** | 0.886 |
| 0.55 | 4052 | 1156 | 675 | 0.778 | 0.857 | **0.816** | 0.899 |
| 0.60 | 3974 | 1234 | 753 | 0.763 | 0.841 | **0.800** | 0.905 |
| 0.65 | 3915 | 1293 | 812 | 0.752 | 0.828 | **0.788** | 0.909 |
| 0.70 | 3841 | 1367 | 886 | 0.738 | 0.813 | **0.773** | 0.914 |
| 0.75 | 3754 | 1454 | 973 | 0.721 | 0.794 | **0.756** | 0.919 |
| 0.80 | 3596 | 1612 | 1131 | 0.690 | 0.761 | **0.724** | 0.925 |
| 0.85 | 3333 | 1875 | 1394 | 0.640 | 0.705 | **0.671** | 0.933 |
| 0.90 | 2722 | 2486 | 2005 | 0.523 | 0.576 | **0.548** | 0.946 |
| 0.95 | 1305 | 3903 | 3422 | 0.251 | 0.276 | **0.263** | 0.969 |

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

**Prevemos 5208 núcleos onde há 4727 — 110.2%.** O pipeline
sobre-segmenta. Com recall de 0.888 a IoU 0,50 encontra quase tudo o que lá
está; com precisão de 0.806, cerca de um quinto do que reporta não
corresponde a nenhum núcleo anotado. O suspeito é o `min_distance`, que semeia o watershed:
foi a constante que o teste de mutação do lote dourado mostrou ser a mais sensível, e
semear duas vezes dentro de um núcleo parte-o em dois.

**Não afinei isto.** A medição acima é sobre o split **test**, e ajustar parâmetros contra
o conjunto de teste é como um número deixa de significar o que diz. O caminho honesto existe
e está aberto: afinar contra o split **validation**, que é para isso que o BBBC039 o publica,
voltar a medir no test uma vez, e recongelar o lote dourado no mesmo commit — porque mudar
o `min_distance` desloca as contagens que o §13.2 guarda.

**Contexto.** Um F1@0,50 de 0.845 para um pipeline clássico por afinar é
razoável e não é estado da arte; a literatura sobre este conjunto põe o CellProfiler na
mesma ordem e as redes convolucionais claramente acima. É o número que dá sentido a ter dois
backends, que é precisamente o argumento do §13.5.

## O que a ground truth precisou de descodificação

As máscaras do BBBC039 não trazem um inteiro por núcleo: dizem **quantos núcleos cobrem cada
pixel** — 0 fundo, 1 um núcleo, 2 e 3 onde vizinhos se sobrepõem. As instâncias são as
componentes ligadas dos 1, crescidas para os pixels disputados por watershed.

Ler o ficheiro ingenuamente dá **três objetos por imagem em vez de cem**, e as 200 máscaras
somariam 531 núcleos em vez de 19 391. Teria produzido uma pontuação bonita e sem sentido —
e nada mais neste repositório teria dado por isso.
