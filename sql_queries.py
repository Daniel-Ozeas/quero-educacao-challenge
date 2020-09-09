# Droping Table
public_dataset_table_drop = 'DROP TABLE IF EXISTS public_dataset'

# Create Table
public_dataset_table_create = ("""
    CREATE TABLE IF NOT EXISTS public_dataset(
                                            categoria INTEGER, \
                                            cbo2002_ocupacao INTEGER, \
                                            competencia INTEGER, \
                                            fonte INTEGER, \
                                            grau_de_instrucao INTEGER, \
                                            horas_contratuais INTEGER, \
                                            id INTEGER PRIMARY KEY NOT NULL, \
                                            idade INTEGER, \
                                            ind_trab_intermitente INTEGER, \
                                            ind_trab_parcial INTEGER,  \
                                            indicador_aprendiz INTEGER, \
                                            municipio INTEGER, \
                                            raca_cor INTEGER, \
                                            regiao INTEGER, \
                                            salario DECIMAL, \
                                            saldo_movimentacao INTEGER, \
                                            secao TEXT, \
                                            sexo INTEGER, \
                                            subclasse INTEGER, \
                                            tam_estab_jan INTEGER, \
                                            tipo_de_deficiencia INTEGER, \
                                            tipo_empregador INTEGER, \
                                            tipo_estabelecimento INTEGER, \
                                            tipo_movimentacao INTEGER, \
                                            uf INTEGER \
    )
""")

# Insert into table

public_dataset_table_insert = ("""
    INSERT INTO public_dataset(
                            categoria, \
                            cbo2002_ocupacao, \
                            competencia, \
                            fonte, \
                            grau_de_instrucao, \
                            horas_contratuais, \
                            id, \
                            idade, \
                            ind_trab_intermitente, \
                            ind_trab_parcial,  \
                            indicador_aprendiz, \
                            municipio, \
                            raca_cor, \
                            regiao, \
                            salario, \
                            saldo_movimentacao, \
                            secao, \
                            sexo, \
                            subclasse, \
                            tam_estab_jan, \
                            tipo_de_deficiencia, \
                            tipo_empregador, \
                            tipo_estabelecimento, \
                            tipo_movimentacao, \
                            uf)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""")

