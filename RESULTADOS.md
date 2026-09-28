# Resultados observados nesta versão

Execução local em Linux com Go 1.22, três processos TCP e 250 snapshots iniciados no processo 0 em cada cenário. A pasta `evidencias/` contém os três arquivos de snapshot, logs e arquivo compartilhado de cada execução. Os números variam com o escalonamento; reproduza no seu computador com os comandos do README.

| Cenário | Snapshots completos | Violações de invariantes | Acessos válidos (normal) | `||` | `..` | Interpretação |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `normal` | 250/250 | 0 | 17.028 | 0 | 0 | Exclusão mútua respeitada nesta execução. Cada processo concluiu acessos (5.692, 5.679, 5.657). |
| `unsafe` | 250/250 | 36 (`INV1`) | — | 5.274 | 5.275 | Dois ou três processos aparecem em `inMX` em snapshots; as escritas se intercalaram. |
| `block` | 250/250 | 250 (`INV6`) | — | 0 | 0 | Ciclo de espera por respostas; nenhum processo acessou o arquivo. |

No cenário normal, o arquivo tinha 34.056 bytes, exatamente 17.028 pares `|.`. Nos cenários com falha, dividir os bytes por dois não representa acessos válidos: o verificador informa apenas quantas posições de dois bytes examinou.

Estes resultados são evidência experimental para os cenários executados. A argumentação do algoritmo depende das hipóteses de membros fixos, comunicação FIFO confiável entre processos ativos e liberação da seção crítica após cada acesso.
