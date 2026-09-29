# Resultados observados nesta versão

Execução local em Linux com Go 1.22, três processos TCP e 250 snapshots iniciados no processo 0 em cada cenário. A pasta `evidencias/` contém os três arquivos de snapshot, logs e arquivo compartilhado de cada execução. Os números variam com o escalonamento; reproduza no seu computador com os comandos do README.

| Cenário | Snapshots completos | Violações de invariantes | Acessos válidos (normal) | `||` | `..` | Interpretação |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `normal` | 250/250 | 0 | 17.028 | 0 | 0 | Exclusão mútua respeitada nesta execução. Cada processo concluiu acessos (5.692, 5.679, 5.657). |
| `unsafe` | 250/250 | 36 (`INV1`) | — | 5.274 | 5.275 | Dois ou três processos aparecem em `inMX` em snapshots; as escritas se intercalaram. |
| `block` | 250/250 | 750 (`INV5`), em 250 snapshots | — | 0 | 0 | Todo processo posterga a resposta sem ter prioridade; ninguém acessou o arquivo. |

No cenário normal, o arquivo tinha 34.056 bytes, exatamente 17.028 pares `|.`. Nos cenários com falha, dividir os bytes por dois não representa acessos válidos: o verificador informa apenas quantas posições de dois bytes examinou.

## Reexecução em macOS

Os mesmos três cenários, rodados com `bash scripts/demo.sh` em macOS 14 (Apple M2) com Go 1.23.2, para confirmar que o resultado não depende do sistema operacional:

| Cenário | Snapshots completos | Violações de invariantes | Acessos válidos (normal) | `||` | `..` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `normal` | 250/250 | 0 | 49.013 | 0 | 0 |
| `unsafe` | 250/250 | 113 (`INV1`), em 113 snapshots | — | 15.130 | 15.323 |
| `block` | 250/250 | 750 (`INV5`), em 250 snapshots | — | 0 | 0 |

No cenário normal, os três processos concluíram 16.384, 16.338 e 16.291 acessos, e o arquivo ficou com 98.026 bytes — exatamente 49.013 pares `|.`, sem nenhuma ocorrência de `||` ou `..`. O número de acessos é bem maior que o da execução em Linux porque a máquina é mais rápida; o que importa é que nenhuma violação apareceu em nenhuma das duas.

A falha `unsafe` aparece com mais frequência nos snapshots quando a seção crítica é mais longa — por isso o `demo.sh` usa `--work=50000` nesse cenário. O laço de CPU não é uma espera (`sleep`): ele apenas alarga a janela em que dois processos podem ser flagrados dentro da seção crítica ao mesmo tempo. Com `--work=0` a falha continua sendo detectada, só que em menos snapshots.

## Observações

Estes resultados são evidência experimental para os cenários executados. A argumentação do algoritmo depende das hipóteses de membros fixos, comunicação FIFO confiável entre processos ativos e liberação da seção crítica após cada acesso.

Ao rodar à mão com `--duration=0` e encerrar com `Ctrl+C`, o arquivo compartilhado pode terminar com um `|` sem o `.` correspondente, se a interrupção pegar um processo dentro da seção crítica. O verificador identifica esse caso e avisa que houve um acesso inacabado; não é violação de exclusão mútua.
