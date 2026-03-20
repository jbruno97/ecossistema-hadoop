# Ecossistema Hadoop

## 1. O que é o Apache Hadoop?

O Apache Hadoop é um framework open-source que permite o processamento distribuído de grandes conjuntos de dados em clusters de computadores. Ele foi projetado para escalar de um único servidor para milhares de máquinas, oferecendo um alto grau de tolerância a falhas.

### Principais Componentes:

1.  **HDFS (Hadoop Distributed File System):** Um sistema de arquivos distribuído que fornece acesso de alto desempenho aos dados de aplicação. É a camada de armazenamento do Hadoop.
2.  **YARN (Yet Another Resource Negotiator):** O gerenciador de recursos do cluster, responsável por alocar CPU e memória para as aplicações.
3.  **MapReduce:** Um modelo de programação para processamento de dados em paralelo. Embora ainda seja usado, o Apache Spark (que veremos no próximo módulo) tornou-se a alternativa mais moderna e eficiente.

[Informações sobre o ecossistema hadoop](https://hadoopecosystemtable.github.io/)

### Laboratórios

Aulas 1 e 2 - Prática com Hadoop no Colab - [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1lsYb7xP6gFk-Zwyzwl0Hkbk3aWIQ5KpK?usp=sharing)

Aula 3 - Prática com Spark no Colab - [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1QlQiUuRNcbWFW3xLuY1BoUWlrPyZGxL8?usp=sharing)

### Dataset
[Brasilian e-commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Outras Sugestões de Dataset
1. https://data.gov/
2. https://data.worldbank.org/
3. https://opendata.cityofnewyork.us/

---

### FAQ — Ecossistema Hadoop

> Clique nas perguntas para expandir 👇

---

<details>
<summary> Preciso sempre usar export JAVA_HOME no Colab?</summary>

👉 **Não, se configurar corretamente.**

Use isso no início do notebook:

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["HADOOP_HOME"] = "/content/hadoop-3.2.1"
```

✔ Execute apenas uma vez

</details>

---

<details>
<summary> Por que o export não funciona direito no Colab?</summary>

* `!comando` → roda em um terminal temporário
* Variáveis desaparecem após execução

👉 O Colab é um ambiente **temporário**

</details>

---

<details>
<summary> Como isso funciona em ambiente real?</summary>

No mundo real, o Java já está configurado:

* Sistema (`/etc/environment`)
* Usuário (`~/.bashrc`)
* Hadoop (`hadoop-env.sh`) ✅

👉 Você não precisa ficar exportando

</details>

---

<details>
<summary> Preciso saber Java para usar Hadoop?</summary>

👉 **Não**

Você pode usar:

* Python (Hadoop Streaming) ✅
* SQL (Hive)
* Scala (Spark)

✔ Java é só o motor interno

</details>

---

<details>
<summary> Onde posso rodar Hadoop?</summary>

| Ambiente             | Uso             |
| -------------------- | --------------- |
| Colab                | Aprender        |
| Docker               | Desenvolvimento |
| Cloud (Dataproc/EMR) | Produção        |
| Cloudera             | Simulação real  |

</details>

---

<details>
<summary> O que é um arquivo pequeno no Hadoop?</summary>

👉 Qualquer arquivo menor que **128 MB**

</details>

---

<details>
<summary> Por que arquivos pequenos são um problema?</summary>

* Sobrecarregam o NameNode
* Consomem memória
* Reduzem performance

👉 Muitos arquivos pequenos = problema sério

</details>

---

<details>
<summary> Quanto cada arquivo consome?</summary>

👉 Aproximadamente **150 bytes de RAM**

</details>

---

<details>
<summary>Como resolver o problema de arquivos pequenos?</summary>

✔ Estratégias:

* Agrupar arquivos
* Sequence Files
* HAR Files
* CombineFileInputFormat

👉 Melhor solução: evitar gerar arquivos pequenos

</details>

---

<details>
<summary> Existe parâmetro para isso no Hadoop?</summary>

Sim:

```bash
mapreduce.input.fileinputformat.split.minsize
```

</details>

---

<details>
<summary> Qual a regra de ouro no Hadoop?</summary>

👉 Trabalhar com arquivos próximos de **128 MB**

</details>

---

<details>
<summary> Explicação simples (analogia)</summary>

* Servidor real → já configurado
* Colab → você configura toda vez

👉 Use `os.environ` como sua “mochila”

</details>

---

<details>
<summary> Dica final do professor</summary>

```text
Colab = aprender lógica
Cloud = mundo real
MapReduce = base da engenharia de dados
```

</details>

---

#  **Se ainda tiver dúvidas...**

👉 Abra uma issue no repositório
👉 Ou traga para a aula
