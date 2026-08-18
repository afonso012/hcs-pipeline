# Afinação da segmentação — split de validação (§13.5)

Gerado em 2026-08-18T22:02:48+00:00 por `scripts/tune_segmentation.py`.
50 campos do split **validation**, 16 configurações.

**Escolhido: `min_distance=20`, `sigma=1.0`** — +0.080 de F1 médio contra a configuração atual (min_distance=10, sigma=1.0).

| min_distance | sigma | F1 médio 0,50:0,95 | F1@0,50 | previstos/anotados |
|---:|---:|---:|---:|---:|
| 20 | 1.0 | 0.754 | 0.881 | 96.7% |  ← escolhido
| 25 | 1.0 | 0.754 | 0.873 | 87.1% |
| 20 | 1.5 | 0.739 | 0.880 | 96.2% |
| 25 | 1.5 | 0.734 | 0.869 | 87.2% |
| 15 | 1.0 | 0.722 | 0.852 | 104.1% |
| 30 | 1.0 | 0.715 | 0.827 | 76.8% |
| 15 | 1.5 | 0.710 | 0.854 | 103.4% |
| 12 | 1.0 | 0.696 | 0.832 | 107.7% |
| 30 | 1.5 | 0.695 | 0.822 | 76.6% |
| 12 | 1.5 | 0.689 | 0.839 | 106.7% |
| 10 | 1.0 | 0.674 | 0.814 | 110.6% |
| 10 | 1.5 | 0.666 | 0.818 | 109.7% |
| 8 | 1.0 | 0.642 | 0.786 | 115.0% |
| 8 | 1.5 | 0.635 | 0.791 | 113.9% |
| 6 | 1.0 | 0.580 | 0.731 | 124.0% |
| 6 | 1.5 | 0.578 | 0.740 | 121.8% |

## A regra, e porque está escrita aqui

A escolha acima é feita sobre **validation**. O número que se cita é medido sobre **test**,
uma vez, depois desta escolha estar fechada. O BBBC039 publica os dois splits exatamente
para isto.

A alternativa — ajustar, medir no test, repetir — produz um número que descreve o ruído
deste conjunto e não o método. É a mesma proibição que o §13.2 escreve para o lote dourado,
e não é mais defensável aqui do que lá.

**Uma passagem.** Esta varredura corre uma vez e o resultado aceita-se. Iterar até o número
ficar bonito é afinar a olhar para a resposta com passos extra.

## O que muda a jusante

`min_distance` desloca as contagens de células, e as contagens de células são o que o lote
dourado do §13.2 guarda a 2%. Mudar esta constante obriga a **recongelar a referência no
mesmo commit** — é para isso que a regra do §13.2 existe, e o diff da referência passa a ser
a documentação da mudança.
