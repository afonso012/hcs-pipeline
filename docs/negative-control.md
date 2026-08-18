# Controlo negativo — Objetivo B (§21.3, ponto 4)

Gerado em 2026-08-18T13:05:51+00:00 por `scripts/negative_control.py`.
Lâmina `CMU-1.svs`, nível 0: 46000x32914, **5,64 GiB** em RGBA. Limite do contentor:
1536m, sem swap.

| modo | limite | saída | OOMKilled | segundos | veredicto |
|---|---|---:|---|---:|---|
| whole | 1536m | 137 | sim | 6.1 | morto pelo limite |
| chunked | 1536m | 0 | não | 373.8 | concluído |

## Porque é que isto existe

Todas as outras medições deste projeto dizem que o consumo é baixo. **Nenhuma delas prova
que a montagem seria capaz de detetar consumo alto**, e uma medição que não pode falhar não
é prova de nada.

Daí a mesma lâmina, no mesmo nível, sob o mesmo limite, de duas maneiras. O modo `whole` é
o `read_region` sobre o nível 0 inteiro — não é um espantalho, é a linha que qualquer
pessoa escreve primeiro, e o OpenSlide tenta-a. O modo `chunked` é o pipeline sobre os
mesmos pixels.

**O par é que é o argumento.** Um falhar prova que o limite é real; o outro concluir prova
que o desenho é o que sobrevive a ele. Qualquer um deles sozinho não prova nada.

## O limite é de contentor, não `setrlimit`

O `RLIMIT_AS` limita espaço de endereçamento virtual, que não é o que o deployment impõe
nem o que mata um worker em produção. O que mata é um limite de cgroup e um OOMKill com
saída 137, e o §15 conta com esse mecanismo. Este controlo verifica que ele dispara.

A saída 137 sozinha também não bastaria: é SIGKILL, e o OOM killer é só uma das coisas que
o enviam. A coluna `OOMKilled` vem do `docker inspect` e é a que diz qual.

## Veredicto

**Passa.** O caminho ingénuo morreu no limite e o caminho por blocos concluiu.
