# Databricks notebook source

# MAGIC %md
# MAGIC # Delta Lake na prática — demos da aula
# MAGIC **Semana 2 · Dia 1 · Delta Lake e Medallion Architecture**
# MAGIC
# MAGIC Este notebook é **autossuficiente**: ele cria o próprio schema e todas as tabelas de exemplo com dados sintéticos. Não depende de nenhuma tabela ou arquivo prévio.
# MAGIC
# MAGIC | Parte | Tema | Parada no deck |
# MAGIC |---|---|---|
# MAGIC | 0 | Setup (cria o schema de trabalho) | — |
# MAGIC | 1 | `_delta_log` por dentro | Parada para demo 1 (depois de "Ler, gravar, validar e commitar") |
# MAGIC | 2 | ACID em demo (A, C, I, D) | Demo 2 (depois de "ACID") |
# MAGIC | 3 | Schema enforcement x schema evolution | Demo 3 (depois de "Enforcement ou evolution?") |
# MAGIC | 4 | MERGE (upsert) | Demo 4 (depois de "MERGE, OPTIMIZE e VACUUM") |
# MAGIC | 5 | Time travel e RESTORE | Demo 5 (depois de "Histórico, consulta por versão e RESTORE") |
# MAGIC | 6 | OPTIMIZE e Liquid Clustering | Demos 6 e 12 (6.1 depois de "Limites e Automatic Liquid Clustering") |
# MAGIC | 7 | VACUUM | Demo 7 |
# MAGIC | 8 | Limpeza | — |
# MAGIC
# MAGIC **Como rodar:** anexe a um compute (serverless ou clássico, DBR 15.4+ recomendado) e execute célula a célula. Os outputs são parte da aula: pare, pergunte à turma o que vai acontecer antes de rodar.
# MAGIC
# MAGIC O segundo notebook (`02_medallion_scd_cdc_cdf`) cobre Medallion (lab bronze → silver), SCD, CDC e Change Data Feed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 0 — Setup
# MAGIC Os widgets abaixo definem onde as tabelas serão criadas. Deixando **vazio**, usamos o catálogo padrão do seu workspace e criamos o schema `demo_delta_<seu_usuario>`.
# MAGIC
# MAGIC > Se o seu ambiente (ex.: lab da Academy) não permite criar schemas, preencha `catalogo` e `schema` com um local onde você tem permissão de escrita.

# COMMAND ----------

import re
from pyspark.sql import functions as F

try:
    usuario = spark.sql("SELECT current_user()").first()[0]
except Exception:
    usuario = "aluno"

dbutils.widgets.text("catalogo", "", "Catálogo (vazio = padrão)")
dbutils.widgets.text("schema", "", "Schema (vazio = demo_delta_<usuario>)")
dbutils.widgets.dropdown("limpar_no_final", "nao", ["sim", "nao"], "Apagar o schema no final?")

catalogo = dbutils.widgets.get("catalogo").strip()
schema = dbutils.widgets.get("schema").strip() or "demo_delta_" + re.sub(r"[^a-zA-Z0-9]", "_", usuario.split("@")[0])

if catalogo:
    spark.sql(f"USE CATALOG `{catalogo}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

catalogo_atual = spark.sql("SELECT current_catalog()").first()[0]
print(f"Trabalhando em: {catalogo_atual}.{schema}")

# COMMAND ----------

# Funções auxiliares usadas ao longo do notebook
def historico(tabela, n=10):
    """Mostra as últimas versões do DESCRIBE HISTORY de forma compacta."""
    return (spark.sql(f"DESCRIBE HISTORY {tabela}")
            .select("version", "timestamp", "operation", "operationParameters", "operationMetrics")
            .orderBy(F.col("version").desc())
            .limit(n))

def detalhe(tabela):
    return spark.sql(f"DESCRIBE DETAIL {tabela}").select("format", "numFiles", "sizeInBytes", "clusteringColumns", "properties")

def tentar(titulo, funcao):
    """Executa algo que DEVE falhar e mostra o erro de forma didática."""
    try:
        funcao()
        print(f"[{titulo}] ⚠️ não falhou (inesperado)")
    except Exception as e:
        msg = str(e).strip().split("\n")[0][:300]
        print(f"[{titulo}] ✅ falhou como esperado:\n    {type(e).__name__}: {msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — O `_delta_log` por dentro
# MAGIC **Ideia:** uma tabela Delta é uma pasta com arquivos Parquet **mais** uma pasta `_delta_log/` com um arquivo JSON por commit. Quem "manda" na tabela é o log, não a pasta de dados.
# MAGIC
# MAGIC Para conseguir **listar os arquivos**, criamos esta tabela por *caminho* (em um Volume do Unity Catalog). Tabelas gerenciadas do UC escondem o caminho físico.

# COMMAND ----------

import tempfile

caminho_log = None
try:
    spark.sql("CREATE VOLUME IF NOT EXISTS demo_volume")
    caminho_log = f"/Volumes/{catalogo_atual}/{schema}/demo_volume/tabela_log_demo"
    dbutils.fs.mkdirs(caminho_log)
except Exception as e:
    caminho_log = tempfile.mkdtemp() + "/tabela_log_demo"
    print("Volume indisponível; usando caminho alternativo:", caminho_log)

print("Caminho da tabela:", caminho_log)

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE delta.`{caminho_log}` (id BIGINT, valor BIGINT)
    USING DELTA
    TBLPROPERTIES (delta.enableDeletionVectors = false)
""")   # commit 0: cria a tabela

spark.sql(f"INSERT INTO delta.`{caminho_log}` SELECT id, id * 10 FROM range(1, 6)")   # commit 1: 5 linhas
spark.sql(f"UPDATE delta.`{caminho_log}` SET valor = 999 WHERE id = 1")               # commit 2: update

def listar(caminho):
    arquivos = dbutils.fs.ls(caminho)
    for a in sorted(arquivos, key=lambda x: x.name):
        print(f"{a.size:>8} bytes  {a.name}")

print("== Pasta da tabela ==")
listar(caminho_log)
print("\n== Pasta _delta_log ==")
listar(caminho_log + "/_delta_log")

# COMMAND ----------

# MAGIC %md
# MAGIC **O que olhar:** arquivos `.parquet` (dados) e, em `_delta_log`, um `.json` por commit (`000...000.json`, `000...001.json`, `000...002.json`). O número do arquivo é a **versão** da tabela.
# MAGIC
# MAGIC Agora abrimos o commit 2 (o `UPDATE`): um UPDATE em Delta **não altera** o Parquet; ele marca o arquivo antigo como removido (`remove`) e adiciona um novo (`add`).

# COMMAND ----------

commit2 = spark.read.json(f"{caminho_log}/_delta_log/00000000000000000002.json")
print("Colunas (tipos de ação) presentes no commit 2:", commit2.columns)

(commit2.select(
    F.col("commitInfo.operation").alias("operacao"),
    F.col("add.path").alias("arquivo_adicionado"),
    F.col("remove.path").alias("arquivo_removido"))
 .show(truncate=False))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{caminho_log}`")
        .select("version", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pergunta para a turma:** se o arquivo antigo continua na pasta, por que um `SELECT` normal não o enxerga?
# MAGIC *Resposta:* porque o leitor reconstrói o estado da tabela a partir do log (soma dos `add` menos os `remove`). Arquivo fora do log = arquivo invisível. É isso que torna o time travel possível (e o `VACUUM` necessário).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — ACID em demo
# MAGIC Vamos provar cada letra com um experimento. Tabelas desta parte: `contas` e `acid_pedidos`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE acid_pedidos (id BIGINT, valor DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO acid_pedidos VALUES (1, 100.0), (2, 250.0), (3, 80.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM acid_pedidos ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Atomicidade — ou tudo ou nada
# MAGIC Vamos inserir 10 linhas, mas forçamos um erro na linha 7. Algumas tarefas Spark podem até escrever Parquet, mas **sem o commit no log nada é visível**.

# COMMAND ----------

versao_antes = spark.sql("DESCRIBE HISTORY acid_pedidos").agg(F.max("version")).first()[0]

tentar("INSERT com falha na linha 7", lambda: spark.sql("""
    INSERT INTO acid_pedidos
    SELECT id + 100,
           CASE WHEN id = 7 THEN CAST(raise_error('falha simulada na linha 7') AS DOUBLE) ELSE id * 1.0 END
    FROM range(1, 11)
"""))

versao_depois = spark.sql("DESCRIBE HISTORY acid_pedidos").agg(F.max("version")).first()[0]
print(f"Versão antes: {versao_antes} | depois: {versao_depois}  (iguais = nenhum commit aconteceu)")
print("Linhas na tabela:", spark.table("acid_pedidos").count(), "(continuam 3, nenhuma linha parcial)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Consistência — constraints protegem as regras do negócio
# MAGIC Criamos `contas` com 3 contas e uma constraint `saldo >= 0`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE contas (conta_id INT, titular STRING, saldo DOUBLE);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO contas VALUES (1, 'Ana', 1000.0), (2, 'Bruno', 500.0), (3, 'Carla', 300.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE contas ADD CONSTRAINT saldo_nao_negativo CHECK (saldo >= 0);

# COMMAND ----------

tentar("saque maior que o saldo", lambda: spark.sql("UPDATE contas SET saldo = saldo - 5000 WHERE conta_id = 2"))
display(spark.table("contas").orderBy("conta_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Isolamento — escritores concorrentes sem corromper dados
# MAGIC Duas "threads" fazem transferências em sentidos opostos entre as **mesmas** contas, ao mesmo tempo. Cada transferência é **um único UPDATE** (um único commit), então débito e crédito andam juntos.
# MAGIC
# MAGIC O Delta usa **controle de concorrência otimista**: ninguém trava ninguém. Quando dois commits conflitam, um deles falha ao tentar commitar e precisa **tentar de novo**. No fim, a soma dos saldos tem que continuar igual.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor
import time, random

soma_inicial = spark.table("contas").agg(F.sum("saldo")).first()[0]
versao_inicial = spark.sql("DESCRIBE HISTORY contas").agg(F.max("version")).first()[0]
conflitos = []   # cada conflito detectado entra aqui

def transferir(origem, destino, valor, tentativas=30):
    for t in range(tentativas):
        try:
            spark.sql(f"""
                UPDATE contas
                SET saldo = CASE WHEN conta_id = {origem} THEN saldo - {valor} ELSE saldo + {valor} END
                WHERE conta_id IN ({origem}, {destino})
            """)
            return True
        except Exception as e:
            conflitos.append(type(e).__name__)
            time.sleep(random.uniform(0.05, 0.3))
    raise RuntimeError("não conseguiu commitar após várias tentativas")

def trabalhador(origem, destino, valor, n):
    return sum(transferir(origem, destino, valor) for _ in range(n))

with ThreadPoolExecutor(max_workers=2) as pool:
    f1 = pool.submit(trabalhador, 1, 2, 10.0, 8)   # Ana -> Bruno: 8 x 10
    f2 = pool.submit(trabalhador, 2, 1, 5.0, 8)    # Bruno -> Ana: 8 x 5
    print("Transferências concluídas:", f1.result(), "e", f2.result())

soma_final = spark.table("contas").agg(F.sum("saldo")).first()[0]
print(f"Soma inicial = {soma_inicial} | soma final = {soma_final} | conflitos detectados e repetidos: {len(conflitos)}")
print("Tipos de conflito:", sorted(set(conflitos)) or "nenhum nesta execução (rode de novo!)")
display(spark.table("contas").orderBy("conta_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Resultado esperado: Ana = 1000 − 80 + 40 = **960**, Bruno = 500 + 80 − 40 = **540**, Carla = **300**. Soma constante (1800). O número de conflitos varia a cada execução.
# MAGIC
# MAGIC **Leitores nunca bloqueiam e sempre veem um snapshot consistente.** Vamos ler a tabela como ela estava *antes* das transferências, mesmo depois de tudo ter mudado:

# COMMAND ----------

display(spark.sql(f"SELECT conta_id, titular, saldo FROM contas VERSION AS OF {versao_inicial} ORDER BY conta_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Durabilidade — o que foi commitado fica
# MAGIC Cada commit está gravado no armazenamento de objetos (S3/ADLS/GCS) como um arquivo JSON imutável. Não existe "commit em memória". O histórico abaixo é a prova.

# COMMAND ----------

display(historico("contas", 20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Schema enforcement x schema evolution
# MAGIC - **Enforcement** (padrão): o Delta **rejeita** escritas que não casam com o schema da tabela.
# MAGIC - **Evolution**: você **autoriza** explicitamente a mudança do schema.
# MAGIC
# MAGIC Tabela desta parte: `clientes`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE clientes (id INT, nome STRING, idade INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO clientes VALUES (1, 'Ana', 30), (2, 'Bruno', 41);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Enforcement — o que é barrado

# COMMAND ----------

# (a) coluna extra que não existe na tabela
df_extra = spark.createDataFrame([(3, "Carla", 28, "carla@exemplo.com")], "id INT, nome STRING, idade INT, email STRING")
tentar("coluna extra 'email'", lambda: df_extra.write.format("delta").mode("append").saveAsTable("clientes"))

# (b) tipo incompatível (array no lugar de int)
df_tipo = spark.createDataFrame([(4, "Davi", [1, 2])], "id INT, nome STRING, idade ARRAY<INT>")
tentar("tipo incompatível em 'idade'", lambda: df_tipo.write.format("delta").mode("append").saveAsTable("clientes"))

# (c) NOT NULL
spark.sql("ALTER TABLE clientes ALTER COLUMN nome SET NOT NULL")
tentar("NULL em coluna NOT NULL", lambda: spark.sql("INSERT INTO clientes VALUES (5, NULL, 22)"))

print("Linhas na tabela (nada entrou):", spark.table("clientes").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Evolution — mudando o schema de propósito
# MAGIC Três formas, da mais explícita para a mais automática:
# MAGIC 1. `ALTER TABLE ... ADD COLUMNS` (você decide, via DDL)
# MAGIC 2. `.option("mergeSchema", "true")` em uma escrita (a escrita decide)
# MAGIC 3. `MERGE` com *auto merge* (a origem decide)

# COMMAND ----------

# 1) DDL explícito
spark.sql("ALTER TABLE clientes ADD COLUMNS (cidade STRING)")

# 2) mergeSchema na escrita: a coluna 'email' passa a existir
(df_extra.write.format("delta").mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("clientes"))

display(spark.table("clientes").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Linhas antigas ficam com `NULL` nas colunas novas — **sem reescrever nenhum arquivo** (é só uma mudança de metadados no log).
# MAGIC
# MAGIC #### O perigo do `mergeSchema` sempre ligado
# MAGIC Se o produtor errar o nome de uma coluna (`emial` em vez de `email`), com evolução automática o erro **vira coluna nova** em vez de ser barrado:

# COMMAND ----------

df_typo = spark.createDataFrame([(6, "Eva", 35, "eva@exemplo.com")], "id INT, nome STRING, idade INT, emial STRING")
(df_typo.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("clientes"))
print("Colunas agora:", spark.table("clientes").columns, "  <-- 'emial' entrou por engano")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Evolution no MERGE
# MAGIC A documentação da Databricks recomenda habilitar a evolução **por operação** (`MERGE WITH SCHEMA EVOLUTION` no SQL, ou `.withSchemaEvolution()` na API Python) em vez de ligar a configuração de sessão `autoMerge`. Com ela, colunas novas da origem são adicionadas ao destino durante `UPDATE SET *` / `INSERT *`.

# COMMAND ----------

origem = spark.createDataFrame([(1, "Ana", 31, "ana@exemplo.com", "Madri", "premium")],
                               "id INT, nome STRING, idade INT, email STRING, cidade STRING, plano STRING")
origem.createOrReplaceTempView("clientes_origem")

try:
    # forma recomendada: evolução só nesta operação
    spark.sql("""
        MERGE WITH SCHEMA EVOLUTION INTO clientes t
        USING clientes_origem s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
except Exception as e:
    # runtimes mais antigos não têm a sintaxe: usamos a configuração de sessão e a desligamos logo depois
    print("Sintaxe por operação indisponível neste runtime; usando autoMerge de sessão.")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.sql("""
        MERGE INTO clientes t
        USING clientes_origem s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

print("Colunas depois do MERGE:", spark.table("clientes").columns)
display(spark.table("clientes").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quando usar cada um
# MAGIC
# MAGIC | Situação | Use | Por quê |
# MAGIC |---|---|---|
# MAGIC | **Bronze** recebendo JSON/CSV de fontes que mudam | **Evolution** (`mergeSchema`, ou Auto Loader com `addNewColumns`) | Bronze deve aceitar tudo e nunca perder dado cru |
# MAGIC | **Silver / Gold** consumidas por BI e ML | **Enforcement** + mudanças via `ALTER TABLE` revisadas | O contrato do schema precisa ser estável; quebra deve ser visível |
# MAGIC | Nova coluna **planejada** (mudança de negócio) | `ALTER TABLE ADD COLUMNS` | Mudança intencional, versionada em código |
# MAGIC | Origem mandou coluna que **não deveria existir** | **Enforcement** (deixe falhar) | Erro de digitação ou mudança não combinada com o time |
# MAGIC | Mudança de **tipo** de coluna / renomear / remover | `ALTER TABLE` (com column mapping) ou `overwriteSchema` | `mergeSchema` só adiciona e faz alguns alargamentos de tipo |
# MAGIC
# MAGIC **Regra de bolso:** quanto mais perto da fonte, mais tolerante (evolution). Quanto mais perto do consumidor, mais rígido (enforcement).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — MERGE (upsert)
# MAGIC Cenário: `produtos_destino` é o catálogo atual. Chega um arquivo de atualização (`produtos_origem`) com:
# MAGIC - id 2: preço mudou → **UPDATE**
# MAGIC - id 3: igual → **nada a fazer**
# MAGIC - id 6 e 7: novos → **INSERT**
# MAGIC - ids 1, 4, 5: não vieram no arquivo → **marcar como inativos** (`WHEN NOT MATCHED BY SOURCE`)

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE produtos_destino (id INT, nome STRING, preco DOUBLE, ativo BOOLEAN)
""")
spark.sql("""
    INSERT INTO produtos_destino VALUES
      (1, 'Caneta', 3.50, true), (2, 'Caderno', 18.00, true), (3, 'Mochila', 120.00, true),
      (4, 'Lápis', 1.20, true), (5, 'Borracha', 0.90, true)
""")
v_inicial = spark.sql("DESCRIBE HISTORY produtos_destino").agg(F.max("version")).first()[0]   # versão com os 5 produtos originais

spark.createDataFrame(
    [(2, "Caderno", 19.90, True), (3, "Mochila", 120.00, True), (6, "Régua", 4.00, True), (7, "Estojo", 25.00, True)],
    "id INT, nome STRING, preco DOUBLE, ativo BOOLEAN"
).createOrReplaceTempView("produtos_origem")

display(spark.table("produtos_destino").orderBy("id"))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO produtos_destino AS t
# MAGIC USING produtos_origem AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND t.preco <> s.preco THEN
# MAGIC   UPDATE SET t.preco = s.preco
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, nome, preco, ativo) VALUES (s.id, s.nome, s.preco, s.ativo)
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   UPDATE SET t.ativo = false

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM produtos_destino ORDER BY id

# COMMAND ----------

# As métricas do MERGE ficam no histórico
display(spark.sql("DESCRIBE HISTORY produtos_destino")
        .where("operation = 'MERGE'")
        .select("version", "operation", F.col("operationMetrics.numTargetRowsInserted").alias("inseridas"),
                F.col("operationMetrics.numTargetRowsUpdated").alias("atualizadas"),
                F.col("operationMetrics.numTargetRowsDeleted").alias("removidas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 A regra de ouro: 1 linha da origem por linha do destino
# MAGIC Se a origem tem **duas linhas** para o mesmo `id`, o Delta não sabe qual aplicar e **recusa o MERGE**.

# COMMAND ----------

spark.createDataFrame(
    [(2, "Caderno", 21.00, True), (2, "Caderno", 22.50, True)],
    "id INT, nome STRING, preco DOUBLE, ativo BOOLEAN"
).createOrReplaceTempView("origem_duplicada")

tentar("MERGE com origem duplicada", lambda: spark.sql("""
    MERGE INTO produtos_destino t USING origem_duplicada s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.preco = s.preco
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Correção:** deduplicar a origem antes (aqui: ficar com a linha de maior preço, como exemplo). Em pipelines reais use uma coluna de ordenação, como timestamp de atualização.

# COMMAND ----------

from pyspark.sql.window import Window

dedup = (spark.table("origem_duplicada")
         .withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("preco").desc())))
         .where("rn = 1").drop("rn"))
dedup.createOrReplaceTempView("origem_dedup")

spark.sql("""
    MERGE INTO produtos_destino t USING origem_dedup s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.preco = s.preco
""")
display(spark.table("produtos_destino").where("id = 2"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 O mesmo MERGE com a API Python (`DeltaTable`)

# COMMAND ----------

from delta.tables import DeltaTable

destino = DeltaTable.forName(spark, "produtos_destino")
(destino.alias("t")
    .merge(spark.table("produtos_origem").alias("s"), "t.id = s.id")
    .whenMatchedUpdate(condition="t.preco <> s.preco", set={"preco": "s.preco"})
    .whenNotMatchedInsertAll()
    .execute())
print("MERGE via DeltaTable executado (idempotente: rodar de novo não duplica nada).")
display(spark.table("produtos_destino").orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 5 — Time travel e RESTORE
# MAGIC Cada commit do `produtos_destino` é uma versão que você pode consultar. Lembre: o log guarda **quais arquivos** compunham a tabela em cada versão.

# COMMAND ----------

display(historico("produtos_destino", 20).select("version", "timestamp", "operation"))

# COMMAND ----------

# A tabela como ela era logo após o INSERT inicial (guardamos o número da versão em v_inicial)
print("Versão inicial:", v_inicial)
display(spark.sql(f"SELECT * FROM produtos_destino VERSION AS OF {v_inicial} ORDER BY id"))

# COMMAND ----------

# Também funciona por timestamp: pegamos o horário do commit da versão inicial
ts_v1 = spark.sql("DESCRIBE HISTORY produtos_destino").where(f"version = {v_inicial}").first()["timestamp"]
print("Timestamp da versão inicial:", ts_v1)
display(spark.sql(f"SELECT * FROM produtos_destino TIMESTAMP AS OF '{ts_v1}' ORDER BY id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 O que mudou entre duas versões? (diff via EXCEPT)

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'antes' AS lado, * FROM (SELECT * FROM produtos_destino VERSION AS OF {v_inicial} EXCEPT SELECT * FROM produtos_destino)
    UNION ALL
    SELECT 'depois' AS lado, * FROM (SELECT * FROM produtos_destino EXCEPT SELECT * FROM produtos_destino VERSION AS OF {v_inicial})
    ORDER BY id, lado
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Desastre e recuperação: `DELETE` sem `WHERE` + `RESTORE`

# COMMAND ----------

versao_segura = spark.sql("DESCRIBE HISTORY produtos_destino").agg(F.max("version")).first()[0]
print("Versão segura (antes do acidente):", versao_segura)

spark.sql("DELETE FROM produtos_destino")          # ops!
print("Linhas depois do acidente:", spark.table("produtos_destino").count())

# COMMAND ----------

spark.sql(f"RESTORE TABLE produtos_destino TO VERSION AS OF {versao_segura}")
print("Linhas depois do RESTORE:", spark.table("produtos_destino").count())
display(historico("produtos_destino", 4).select("version", "operation"))

# COMMAND ----------

# MAGIC %md
# MAGIC Note que o `RESTORE` **não apaga** o histórico: ele cria uma nova versão que aponta para os arquivos antigos. Time travel só funciona enquanto os arquivos antigos existirem — e é o **VACUUM** (Parte 7) que os remove.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 6 — OPTIMIZE e Liquid Clustering
# MAGIC Problema dos **arquivos pequenos**: ingestões frequentes geram muitos arquivos minúsculos e as consultas ficam lentas (abrir 400 arquivos de 5 KB é pior que abrir 1 de 2 MB).
# MAGIC
# MAGIC Vamos provocar o problema com 40 inserts pequenos.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE eventos (evento_id BIGINT, usuario_id INT, tipo STRING, valor DOUBLE)")

for i in range(40):
    (spark.range(i * 50, (i + 1) * 50)
        .select(F.col("id").alias("evento_id"),
                (F.col("id") % 25).cast("int").alias("usuario_id"),
                F.when(F.col("id") % 3 == 0, "compra").when(F.col("id") % 3 == 1, "visita").otherwise("clique").alias("tipo"),
                (F.col("id") % 100 * 1.5).alias("valor"))
        .write.format("delta").mode("append").saveAsTable("eventos"))

print("Linhas:", spark.table("eventos").count())
display(detalhe("eventos").select("numFiles", "sizeInBytes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE eventos

# COMMAND ----------

display(detalhe("eventos").select("numFiles", "sizeInBytes"))
display(spark.sql("DESCRIBE HISTORY eventos")
        .where("operation = 'OPTIMIZE'")
        .select("version", F.col("operationMetrics.numRemovedFiles").alias("arquivos_removidos"),
                F.col("operationMetrics.numAddedFiles").alias("arquivos_adicionados")))

# COMMAND ----------

# MAGIC %md
# MAGIC `numFiles` caiu de ~40 para poucos arquivos. O OPTIMIZE é só **mais um commit**: os arquivos pequenos viram `remove` no log, mas continuam fisicamente na pasta (até o VACUUM) — por isso o time travel ainda funciona depois dele.
# MAGIC
# MAGIC ### 6.1 Liquid Clustering
# MAGIC Em vez de partições (`PARTITIONED BY`) ou `ZORDER`, declaramos **chaves de clustering**. O Delta organiza os arquivos de acordo com elas durante o `OPTIMIZE`, e dá para **trocar as chaves depois** sem recriar a tabela.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE eventos_lc
# MAGIC CLUSTER BY (usuario_id)
# MAGIC AS SELECT * FROM eventos

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE eventos_lc

# COMMAND ----------

display(detalhe("eventos_lc").select("clusteringColumns", "numFiles"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Os padrões de consulta mudaram? Troque as chaves sem reescrever a tabela inteira
# MAGIC ALTER TABLE eventos_lc CLUSTER BY (tipo, usuario_id)

# COMMAND ----------

display(detalhe("eventos_lc").select("clusteringColumns"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **No Databricks** (com Unity Catalog e *predictive optimization*) você ainda pode usar `CLUSTER BY AUTO` e deixar a plataforma escolher e ajustar as chaves, e o OPTIMIZE roda automaticamente. Não combine Liquid Clustering com `PARTITIONED BY` ou `ZORDER` na mesma tabela.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 7 — VACUUM
# MAGIC O `VACUUM` remove **fisicamente** os arquivos que não fazem mais parte de nenhuma versão retida (o padrão é manter **7 dias**). Depois dele, o time travel para versões antigas **deixa de funcionar**.
# MAGIC
# MAGIC Primeiro, o jeito seguro: `DRY RUN` só lista o que seria apagado.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM eventos DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC Nada a listar: os arquivos pequenos substituídos pelo OPTIMIZE têm poucos minutos, e a retenção padrão é de 7 dias.
# MAGIC
# MAGIC ### 7.1 Demonstração destrutiva (só em tabela de teste!)
# MAGIC Para a aula, vamos **burlar a trava de segurança** e vacuar com retenção zero. **Nunca faça isso em produção:** leitores/streams em andamento e o time travel quebram.

# COMMAND ----------

# Em computes serverless essa configuração pode não estar disponível; nesse caso a célula só avisa e segue.
try:
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql("ALTER TABLE eventos SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours')")
    display(spark.sql("VACUUM eventos RETAIN 0 HOURS DRY RUN"))
    retencao_zero_ok = True
except Exception as e:
    retencao_zero_ok = False
    print("Não foi possível desligar a trava neste compute:", str(e).split("\n")[0][:200])
    print("Use um compute clássico para ver a parte destrutiva; o DRY RUN acima já mostra o conceito.")

# COMMAND ----------

if retencao_zero_ok:
    spark.sql("VACUUM eventos RETAIN 0 HOURS")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")   # religa a trava
    print("Tabela atual continua legível:", spark.table("eventos").count(), "linhas")
    tentar("time travel para a versão 1 após VACUUM", lambda: spark.sql("SELECT count(*) FROM eventos VERSION AS OF 1").show())

# COMMAND ----------

# MAGIC %md
# MAGIC **Resumo da Parte 7:**
# MAGIC - `OPTIMIZE` **junta** arquivos (e o log guarda os antigos como removidos).
# MAGIC - `VACUUM` **apaga** os antigos (e o time travel além da retenção deixa de existir).
# MAGIC - Ajuste a retenção com `delta.deletedFileRetentionDuration` pensando em: tempo de recuperação desejado, streams lentos, e custo de armazenamento.
# MAGIC - Para tabelas com **Change Data Feed**, o VACUUM também remove os arquivos de mudanças antigos (veja o notebook 02).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 8 — Limpeza
# MAGIC Escolha `sim` no widget **"Apagar o schema no final?"** e rode a célula abaixo para remover todas as tabelas de demo. Com `nao`, nada é apagado (útil para continuar explorando).

# COMMAND ----------

if dbutils.widgets.get("limpar_no_final") == "sim":
    spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
    print(f"Schema {schema} removido.")
else:
    print("Limpeza ignorada (widget = nao).")
