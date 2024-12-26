# Databricks notebook source
# Capturar o valor do parâmetro passado pela tarefa anterior
job = dbutils.widgets.get("job")
print(f"O job recebido é: {job}")
