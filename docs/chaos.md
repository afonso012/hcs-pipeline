# Teste de caos — §13.4

Gerado em 2026-08-19T10:10:40+00:00 por `scripts/chaos_test.py`.
Run `aca3c0d0-156c-4213-8ded-25023a471a58`, 1 worker(s) mortos com SIGKILL 8s após o
arranque: hcs-worker-1. Reiniciados: hcs-worker-1.

| | invariante | observado |
|---|---|---|
| ✅ | no duplicate rows for (work_unit_id, run_id) | 0 duplicated unit(s); statuses {'DONE': 480} |
| ✅ | processed + failed == total | 480 + 0 = 480 against 480, run COMPLETED |
| ✅ | nothing left stuck in PROCESSING | 0 unit(s) still claimed by a worker that is gone |
| ✅ | permanent failures were not retried | 0 permanent failure(s) with attempts > 1 |

## Porque é SIGKILL

Uma paragem graciosa deixa o Celery terminar a tarefa e devolvê-la à fila com educação, o
que exercita o caminho de encerramento e não o de falha. O caso que interessa é aquele em
que o processo **deixa de existir** entre reclamar uma unidade e marcá-la.

A recuperação que isto verifica é a do próprio Celery: com `task_acks_late`, uma tarefa
morta antes de confirmar volta ao broker e outro worker apanha-a. O que o código tem de
garantir é que apanhá-la outra vez seja **inofensivo** — é a idempotência do §16.2, e a
invariante das linhas duplicadas é onde se veria se não fosse.

## Veredicto

**Aguentam.** Matar workers a meio da corrida não deixou unidades presas, não duplicou linhas, e a contabilidade do run fecha.
