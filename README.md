# Trabalho 1: DiMEx e snapshot de Chandy–Lamport

Projeto em Go para as duas partes do enunciado. A pasta que contém este README e `go.mod` é a **raiz do projeto**. Não é necessário entrar em uma pasta `SD`.

## Componentes e hipóteses

| Requisito | Implementação |
| --- | --- |
| PL sobre comunicação remota | `PP2PLink/PP2PLink.go`: TCP, fila FIFO por destino, confirmação, número de sequência e retransmissão. |
| DiMEx | `DIMEX/DIMEX.go`: Ricart–Agrawala; `ENTER` solicita, `Ind` libera o acesso e `EXIT` envia respostas adiadas. |
| Aplicação concorrente | `cmd/dimex/main.go`: processos independentes escrevem `|` e `.` no mesmo arquivo em chamadas distintas, sem pausa entre elas. |
| Snapshot | O módulo DiMEx registra estado local, envia marcadores e grava mensagens em trânsito de cada canal de entrada até seu marcador. |
| Análise | `cmd/check/main.go`: reúne estados pelo mesmo ID, exige snapshots completos, verifica sete invariantes booleanas e examina o arquivo. |
| Falhas | `--fault=unsafe` responde antes do permitido; `--fault=block` adia todas as respostas. |

O diretório `template/` preserva os arquivos originais da professora como `.go.txt`, sem interferir na compilação. O PL e o DiMEx executáveis foram implementados como módulos próprios. A ordem `|.` segue o tutorial do template (`|` ao entrar e `.` ao sair).

**Hipóteses:** membros e endereços são fixos; os processos participantes continuam ativos; a rede eventualmente permite conexão; todos enxergam o mesmo arquivo (nos testes, na mesma máquina). Não há recuperação de estado após crash e reinício de um processo. A confiabilidade do PL se refere à execução com processos corretos e comunicação que volta a funcionar.

## Rodar no Mac, Linux ou Codespaces

É necessário Go 1.18 ou posterior. Descompacte `DiMeX-Entrega.zip` e abra o terminal **dentro da pasta `DiMeX-Entrega`**, onde aparece `go.mod`. Se o navegador salvou o ZIP em Downloads no Mac:

```bash
cd ~/Downloads
unzip DiMeX-Entrega.zip
cd DiMeX-Entrega
go version
ls
```

Se o Finder já descompactou, faça apenas `cd ~/Downloads/DiMeX-Entrega`. Confira se `ls` mostra `cmd`, `DIMEX`, `PP2PLink` e `scripts`. Execute, **um comando por vez**:

```bash
bash scripts/demo.sh normal
bash scripts/demo.sh unsafe 5200
bash scripts/demo.sh block 5300
```

Cada comando compila os programas, inicia **três processos**, faz o processo 0 iniciar **250 snapshots sucessivos** enquanto também concorre pelo arquivo, encerra os processos e chama o verificador. Portas: 5100–5102, 5200–5202 e 5300–5302. `unsafe` usa mais trabalho de CPU dentro da seção crítica para facilitar a observação da falha; não há `sleep` entre as duas escritas. A pausa inicial serve só para inicialização e a final mantém o módulo respondendo aos últimos pedidos.

O resultado normal deve mostrar `complete snapshots=250`, `incomplete=0`, `invariant violations=0`, `||= 0`, `..= 0` e `invalid positions=0`. O número de pares varia entre computadores. Em `unsafe`, espera-se `INV1` e intercalações no arquivo. Em `block`, espera-se `INV6`, ciclo de espera e zero acessos. A frase `The injected fault was detected in snapshots` indica sucesso da **demonstração da falha**; o verificador isolado retorna código 1 nesses dois casos por encontrar violações.

Se uma execução de `unsafe` tiver intercalação no arquivo, mas nenhum snapshot captar dois processos na seção crítica, o script falhará de propósito: o enunciado pede detecção **pela análise de snapshots**. Repita o comando; essa observação depende do escalonamento.

## Ver os resultados

Cada execução grava separadamente `runs/<cenário>/mxOUT.txt`, `runs/<cenário>/process_0.log` a `process_2.log` e `runs/<cenário>/snapshots/process_0.jsonl` a `process_2.jsonl`. Uma linha JSONL corresponde ao estado de **um processo em um ID de snapshot**; as três linhas com o mesmo `SnapshotID` formam o snapshot global. Para inspecionar:

```bash
wc -l runs/normal/snapshots/process_*.jsonl
cat runs/normal/process_*.log
head -c 80 runs/normal/mxOUT.txt
bin/check --snapshots=runs/normal/snapshots --file=runs/normal/mxOUT.txt --n=3 --min=250
```

`wc -l` deve indicar 250 linhas em **cada** arquivo. O início de `mxOUT.txt` deve alternar `|.|.|.`. Para ler o primeiro estado local e os canais de entrada:

```bash
python3 - <<'PY'
import json
for p in range(3):
    with open(f'runs/normal/snapshots/process_{p}.jsonl') as f:
        s = json.loads(f.readline())
    print('snapshot', s['SnapshotID'], 'processo', p, 'estado', s['Local'])
    print('canais de entrada:', s['Channels'])
PY
```

`evidencias/` contém uma execução já registrada. `runs/` guarda a execução feita no seu computador e é recriada pelo script. Para a apresentação, rode os comandos e mostre os arquivos em `runs/`.

## Por que os algoritmos funcionam

No DiMEx, um pedido tem prioridade por `(timestamp lógico, ID)`. Quem não quer a seção crítica responde imediatamente; quem está nela, ou quer entrar com prioridade maior, adia a resposta. Um processo só recebe `Ind` após obter uma resposta de **todos os outros**. Ao sair, envia as respostas adiadas. Com PL FIFO e processos que eventualmente saem da seção crítica, isso garante exclusão mútua e progresso nas hipóteses acima.

No snapshot de Chandy–Lamport, o primeiro marcador faz o processo salvar seu estado e enviar marcadores por todos os canais de saída. Para cada canal de entrada ainda sem marcador, ele registra as mensagens de protocolo recebidas. O marcador desse canal fecha sua gravação. Os marcadores percorrem a mesma fila FIFO das mensagens de DiMEx destinadas ao mesmo par, preservando a separação entre mensagens anteriores e posteriores ao corte.

## Invariantes de cada snapshot global completo

As funções `Inv1` a `Inv7` retornam `bool`; funções auxiliares explicam as violações e indicam o ID no relatório.

1. **INV1:** no máximo um processo está em `inMX`.
2. **INV2:** se todos estão em `noMX`, não há respostas adiadas nem mensagens de protocolo em trânsito.
3. **INV3:** um pedido adiado em `q` corresponde a um pedido ativo de `p` com o mesmo timestamp.
4. **INV4:** para cada `p` que quer entrar e cada outro `q`, há exatamente uma posição para sua permissão: pedido `p→q` em trânsito, pedido adiado em `q`, resposta `q→p` em trânsito ou resposta já recebida por `p`. A soma sobre os outros processos dá `N−1`.
5. **INV5:** um processo em `noMX` não retém respostas adiadas.
6. **INV6:** o grafo de pedidos aguardando respostas adiadas não tem ciclo.
7. **INV7:** um processo em `inMX` recebeu resposta de todos os outros.

INV4 também conta o **pedido em trânsito**: antes de `q` recebê-lo ainda não pode haver resposta ou flag de espera. Ausência de progresso por alguns segundos, isoladamente, não prova bloqueio permanente; em `block`, o ciclo INV6 identifica a razão do bloqueio.

## Três terminais, passo a passo (opcional)

O script é recomendado porque inicia e encerra os processos de forma coordenada. Para acompanhar cada processo em um terminal, dentro da raiz em todos eles:

```bash
mkdir -p runs/manual/snapshots
: > runs/manual/mxOUT.txt
go build -buildvcs=false -o bin/dimex ./cmd/dimex
go build -buildvcs=false -o bin/check ./cmd/check
```

Terminal 1:

```bash
bin/dimex --duration=15s --snap-count=250 --snapshots=runs/manual/snapshots --file=runs/manual/mxOUT.txt 0 127.0.0.1:5400 127.0.0.1:5401 127.0.0.1:5402
```

Terminal 2 (inicie logo depois):

```bash
bin/dimex --duration=15s --snap-count=250 --snapshots=runs/manual/snapshots --file=runs/manual/mxOUT.txt 1 127.0.0.1:5400 127.0.0.1:5401 127.0.0.1:5402
```

Terminal 3 (inicie logo depois):

```bash
bin/dimex --duration=15s --snap-count=250 --snapshots=runs/manual/snapshots --file=runs/manual/mxOUT.txt 2 127.0.0.1:5400 127.0.0.1:5401 127.0.0.1:5402
```

Ao final:

```bash
bin/check --snapshots=runs/manual/snapshots --file=runs/manual/mxOUT.txt --n=3 --min=250
```

Se demorar para iniciar os três terminais, repita usando o script; a janela de 15 segundos do primeiro processo pode terminar antes dos demais. Todos usam a mesma lista de endereços na mesma ordem e IDs diferentes.

## Entrega no Git

O ZIP é o código e as evidências, **não é um link Git**. Coloque a pasta no repositório do grupo e confira `git status` e `git log --oneline`. A professora exige mais de um commit real, feito durante o desenvolvimento/revisão; não entregue um único commit contendo tudo. Mostrem na apresentação o teste normal, as duas falhas, um estado JSON de cada processo com o mesmo ID, os canais em trânsito e o histórico de commits. `runs/` está ignorado pelo Git; `evidencias/` guarda as amostras que acompanham o código.
