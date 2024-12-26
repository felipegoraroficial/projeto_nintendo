# Databricks notebook source
import subprocess

result = subprocess.run(
    ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
    capture_output=True,
    text=True
)
print(result.stdout)

# COMMAND ----------

import pygit2

repo = pygit2.Repository('nintendo-elt-process/teste2.py')  # Substitua pelo caminho do seu repositório

branch_atual = repo.head.shorthand
print(branch_atual)
