# 📝 Artigo para o LinkedIn (Pronto para Publicação)

> **Dica:** Copie e cole o conteúdo abaixo diretamente no LinkedIn (como publicação com formatação ou artigo). Lembre-se de marcar o professor Allan Siriani e a Fatec!
> - Professor: [@Allan Siriani](https://www.linkedin.com/in/siriani/)
> - Fatec: [@Fatec Pompeia](https://www.linkedin.com/school/fatec-pompeia/) (ou sua unidade Fatec)
> - Link do Repositório: inclua o link do seu GitHub público!

---

### Título / Gancho:
**Do Pacote Bruto ao Dado Útil: Como construí um pipeline de dados IoT resiliente para redes Meshtastic**

---

### Corpo da Publicação:

Ter um banco de dados cheio de pacotes de telemetria não significa que você tem respostas. Em projetos de Internet das Coisas (IoT) e sensores de campo — especialmente operando em malhas de rádio LoRa/Meshtastic —, o dado bruto esconde armadilhas clássicas de engenharia que quebram qualquer visualização ingênua.

Na disciplina de **Arquiteturas Cloud para Big Data / Projeto Integrador** na **Fatec**, sob orientação do professor **Allan Siriani**, enfrentei o desafio da Atividade 10: transformar os pacotes brutos que recebemos via rádio e MQTT em séries limpas, agregadas e consultáveis em tempo real.

No nosso laboratório, operamos a cadeia:
`DHT11 → Arduino Mega → Heltec !8fa724e8 → MQTT → MongoDB (iot.sensores)`
onde nosso nó transmite leituras ambientais e se comunica em malha com os demais nós da turma.

Aqui estão os **quatro problemas do dado bruto de borda** que tratei no pipeline — e como cada um foi modelado e medido:

---

### 1️⃣ Duplicatas de Rádio
Na malha Meshtastic, cada pacote viaja por múltiplos nós (saltos) para vencer distâncias. O mesmo pacote atinge gateways diferentes e é publicado mais de uma vez no MQTT.
👉 **O que fazer:** Deduplicar estritamente pela tupla `(nó de origem, id do pacote)` — nunca por conteúdo. Implementei um cache circular de deduplicação em memória associado a uma restrição `UNIQUE` composta no banco relacional.

### 2️⃣ Relógio Incorreto (Clock Drift / Boot Epoch)
Nós de baixo custo sem GPS nem NTP não sabem que horas são. Eles costumam emitir timestamps iguais a `0` (ano 1970) ou contadores relativos ao instante em que ligaram.
👉 **O que fazer:** A referência temporal da série limpa é sempre o instante oficial de recebimento no gateway (`received_at`). O relógio interno do nó é mantido apenas como metadado de auditoria. Timestamps anômalos são contabilizados automaticamente.

### 3️⃣ Buracos e Atrasos (Gaps de Rede)
Um nó descarregou, entrou em *deep sleep*, teve o cabo desconectado ou perdeu linha de visada.
👉 **O que fazer:** Modelar a ausência! Em engenharia de dados séria, **uma leitura que não veio é informação, e nunca deve ser interpolada em silêncio**. O pipeline monitora o silêncio entre transmissões sucessivas e registra eventos formais de indisponibilidade quando o intervalo supera 30 minutos.

### 4️⃣ Esquemas Mutáveis e Validação de Faixas Físicas
Pacotes de telemetria de hardware (`battery_level`, `voltage`, `channel_utilization`), ambiente (`temperatura`, `umidade` do DHT11) e mensagens customizadas têm formatos distintos e mudam de versão. Além disso, falhas de sensores podem gerar valores fisicamente impossíveis.
👉 **O que fazer:** O pipeline separa por tipo de pacote, valida tipagens e limites físicos (ex: temperatura de -40°C a 85°C, umidade de 0 a 100%, tensão de 2V a 5.5V). O que não bate não é simplesmente descartado: é isolado em uma tabela de **Quarentena**, com motivo catalogado para auditoria. Sem números de rejeição, você não sabe se seu filtro funciona ou se está mascarando bugs.

---

### 🏛️ Três Destinos, Três Perguntas

Para responder as necessidades analíticas e operacionais, distribuí os dados tratados em três camadas:

1. **Série Limpa (MariaDB):** Responde *"O que aconteceu?"*. Utilizei modelagem **EAV (Narrow Table)**: cada métrica vira uma linha `(nó, timestamp, métrica, valor)`. Isso permite acomodar novos sensores e versões de firmware sem precisar rodar comandos bloqueantes de `ALTER TABLE` no banco.
2. **Agregados por Hora e Dia (MariaDB):** Responde *"Como foi o período?"*. Pré-calcula média, mínimo, máximo e contagem por janelas temporais. Otimiza consultas analíticas históricas de temperatura e umidade sem escanear milhões de linhas brutas.
3. **Último Valor em Memória (Redis):** Responde *"E agora?"*. Armazena o snapshot em tempo real de cada nó e métrica com leitura em microssegundos ($O(1)$ em RAM), ideal para dashboards ao vivo sem sobrecarregar o banco relacional.

---

### 🔍 Três Consultas e Resultados Reais da Operação

Com os dados modelados sobre a base real coletada pela turma, obtivemos insights operacionais claros:

1. **Comportamento Diário dos Sensores (DHT11):** Acompanhamos a curva térmica real do nosso nó `!8fa724e8`. No dia 24/10, por exemplo, a temperatura começou em 19.17°C às 08h e escalou até 23.00°C às 17h conforme a sala aquecia, enquanto a umidade relativa caiu de 16% para 14%.
2. **Saturação e Resolução Horária da Rede:** As agregações horárias comprimiram milhares de registros em 70 janelas temporais limpas, permitindo visualizar com precisão a taxa de chegada de leituras sem onerar o banco com consultas pesadas.
3. **Detecção de Silêncio e Gaps:** O pipeline identificou exatamente 2 grandes períodos de silêncio do nó `!8fa724e8` — o maior deles durou **18.9 horas** (entre o fim da tarde de 22/10 e a manhã de 23/10, quando a bancada foi desligada). A ausência foi registrada formalmente sem criar dados falsos.

### 📈 Auditoria de Qualidade dos Dados (Métricas Reais no Redis):
- **Pacotes brutos lidos:** 2.729
- **Duplicatas de rádio eliminadas:** 1.781 (mais de 65% do tráfego da malha eram retransmissões repetidas!)
- **Timestamps inválidos corrigidos:** 39
- **Pacotes isolados na quarentena:** 44 (mensagens de texto cruas sem métricas numéricas)
- **Métricas limpas persistidas:** 1.808 registros na série temporal

Todo o pipeline foi empacotado em **Docker** e construído em **Python** com controle atômico de checkpoint e transações em lote, garantindo reinício resiliente sem perda nem duplicidade.

---

Código-fonte, esquemas SQL completos, Dockerfile e exemplos práticos disponíveis no meu repositório:
🔗 [Inserir link do seu repositório no GitHub aqui]

Agradecimento especial ao professor Allan Siriani e à Fatec pelos desafios práticos de engenharia que nos aproximam do mercado real de Big Data e Computação em Nuvem!

#EngenhariaDeDados #DataEngineering #IoT #Meshtastic #LoRa #Python #MariaDB #Redis #MongoDB #BigData #Cloud #Fatec
