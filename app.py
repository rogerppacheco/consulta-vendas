from flask import Flask, render_template, request
from datetime import datetime
from collections import Counter
import os
from sqlalchemy import create_engine, text

app = Flask(__name__)

# --- MODIFICAÇÃO AQUI ---
# Pega a URL do ambiente de produção ou usa o banco de dados local como padrão
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///vendas.db')
engine = create_engine(DATABASE_URL)

def formatar_data_br(valor_data):
    if not valor_data:
        return ""
    try:
        if isinstance(valor_data, datetime):
            return valor_data.strftime('%d/%m/%Y')
        obj_data = datetime.strptime(str(valor_data).split(' ')[0], '%Y-%m-%d')
        return obj_data.strftime('%d/%m/%Y')
    except (ValueError, TypeError):
        return valor_data
app.jinja_env.filters['formatadata'] = formatar_data_br

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/buscar', methods=['POST'])
def buscar():
    nome_vendedor = request.form['nome_vendedor']
    ano_mes_atual = datetime.now().strftime('%Y-%m')
    
    with engine.connect() as conexao:
        # Lógica para adaptar a query entre SQLite e PostgreSQL
        if engine.dialect.name == 'sqlite':
            query_str = """
                SELECT * FROM vendas WHERE vendedor LIKE :nome_vendedor AND
                (strftime('%Y-%m', dt_pedido) = :ano_mes OR strftime('%Y-%m', dt_inst) = :ano_mes)
            """
        else: # PostgreSQL
            query_str = """
                SELECT * FROM vendas WHERE vendedor ILIKE :nome_vendedor AND
                (to_char(dt_pedido, 'YYYY-MM') = :ano_mes OR to_char(dt_inst, 'YYYY-MM') = :ano_mes)
            """
        
        query = text(query_str)
        resultados = conexao.execute(query, {
            "nome_vendedor": f"%{nome_vendedor}%",
            "ano_mes": ano_mes_atual
        }).fetchall()

    resumo_status = None
    if resultados:
        # Ajuste para acessar colunas por nome
        lista_status = [venda[7] for venda in resultados] # Acessando pelo índice da coluna 'situacao'
        resumo_status = Counter(lista_status)

    return render_template(
        'resultados.html', 
        vendas=resultados, 
        nome_pesquisado=nome_vendedor,
        resumo=resumo_status
    )

if __name__ == '__main__':
    app.run(debug=True)