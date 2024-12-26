# Databricks notebook source
# Acessar o valor do parâmetro 'env'
env = dbutils.widgets.get("env")
print(f"O ambiente é: {env}")
