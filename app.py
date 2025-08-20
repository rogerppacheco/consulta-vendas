from flask import Flask, render_template, request
from datetime import datetime
from collections import Counter
import os
from sqlalchemy import create_engine, text
import pandas as pd

app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///vendas.db')
engine = create_engine(DATABASE_URL)

def formatar_data_br(valor_data):
    if not valor_data: return ""
    try:
        if isinstance(valor_data, datetime): return valor_data.strftime('%d/%m/%Y')
        obj_data = datetime.strptime(str(valor_data).split(' ')[0], '%Y-%m-%d')
        return obj_data.strftime('%d/%m/%Y')
    except (ValueError, TypeError): return valor_data
app.jinja_env.filters['formatadata'] = formatar_data_br

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    # --- Lógica do Relatório Diário (sempre executada) ---
    lista_vendedores = sorted([
        'ALEX', 'ALEXSANDER', 'BRENDA', 'BRUNO FRANÇA', 'CAIO', 'CLAUDIO', 
        'DAIANA', 'EDCARLA', 'EMERSON', 'GABRIEL', 'GEAN', 'GIL', 'GLEICEMAR', 
        'GUSTAVO OLIVEIRA', 'HORRANA', 'INGRID', 'JOSE', 'JULIANA', 'MARLI', 
        'MARLON', 'MICHAEL', 'MOISES', 'PAMELA', 'PAULO', 'ROGERIO', 
        'VIVIANE', 'WALISSON'
    ])
    data_atual = datetime.now().date()
    relatorio_final = []
    total_vl = 0
    total_cc = 0

    try:
        with engine.connect() as conexao:
            # Garante que a coluna dt_pedido está sendo selecionada
            query = text("SELECT vendedor, situacao, metodo_pagamento, dt_pedido FROM vendas")
            df_total = pd.read_sql(query, conexao)
        
        df_total['dt_pedido'] = pd.to_datetime(df_total.get('dt_pedido'), errors='coerce')
        
        df_hoje = df_total[df_total['dt_pedido'].dt.date == data_atual].copy()
        
        vendas_liquidas_hoje = df_hoje[df_hoje['situacao'].isin(['EM ANDAMENTO', 'INSTALADA'])].copy()

        for vendedor in lista_vendedores:
            vendas_do_vendedor = vendas_liquidas_hoje[vendas_liquidas_hoje['vendedor'] == vendedor]
            vl = len(vendas_do_vendedor)
            cc = len(vendas_do_vendedor[vendas_do_vendedor['metodo_pagamento'] == 'CARTÃO DE CRÉDITO'])
            cc_percent = (cc / vl * 100) if vl > 0 else 0
            relatorio_final.append({'vendedor': vendedor, 'vl': vl, 'cc': cc, 'cc_percent': cc_percent})
            total_vl += vl
            total_cc += cc

    except Exception as e:
        print(f"Erro ao gerar relatório diário: {e}")
        relatorio_final = [{'vendedor': v, 'vl': 0, 'cc': 0, 'cc_percent': 0} for v in lista_vendedores]
        total_vl = 0
        total_cc = 0
    total_cc_percent = (total_cc / total_vl * 100) if total_vl > 0 else 0

    # --- Lógica da Busca de Vendas (executada em POST) ---
    resultados = None
    resumo_status = None
    nome_pesquisado = None

    if request.method == 'POST':
        nome_vendedor = request.form['nome_vendedor']
        nome_pesquisado = nome_vendedor
        ano_mes_atual = datetime.now().strftime('%Y-%m')
        with engine.connect() as conexao:
            is_sqlite = engine.dialect.name == 'sqlite'
            # Consulta SQL atualizada para incluir 'periodo' e 'dt_inst'
            query_str = """
                SELECT cliente, vendedor, os, situacao, dt_pedido, periodo, dt_inst FROM vendas 
                WHERE vendedor {} :nome_vendedor AND
                ({}('%Y-%m', dt_pedido) = :ano_mes OR {}('%Y-%m', dt_inst) = :ano_mes)
            """.format(
                'LIKE' if is_sqlite else 'ILIKE',
                'strftime' if is_sqlite else 'to_char',
                'strftime' if is_sqlite else 'to_char'
            )
            query = text(query_str)
            raw_resultados = conexao.execute(query, {"nome_vendedor": f"%{nome_vendedor}%", "ano_mes": ano_mes_atual}).mappings().all()
            
            resultados = [dict(row) for row in raw_resultados]

        resumo_status = Counter([venda['situacao'] for venda in resultados]) if resultados else None

    return render_template(
        'dashboard.html', 
        relatorio=relatorio_final, 
        total_vl=total_vl, 
        total_cc=total_cc, 
        total_cc_percent=total_cc_percent,
        vendas=resultados,
        nome_pesquisado=nome_pesquisado,
        resumo=resumo_status
    )

if __name__ == '__main__':
    app.run(debug=True)