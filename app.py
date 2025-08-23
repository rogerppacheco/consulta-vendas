import os
import pandas as pd
from datetime import datetime
from collections import Counter
from sqlalchemy import create_engine, text

from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

# --- 1. CONFIGURAÇÃO INICIAL ---
app = Flask(__name__)
app.config['SECRET_KEY'] = 'uma-chave-secreta-muito-dificil-de-adivinhar'
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///vendas.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'admin_login' # Redireciona para esta rota se não estiver logado
login_manager.login_message = "Por favor, faça login para acessar esta página."

# --- 2. MODELOS DO BANCO DE DADOS ---

# UserMixin fornece implementações padrão para métodos que o Flask-Login espera.
class AdminUser(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class VisibleSeller(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    seller_name = db.Column(db.String(100), unique=True, nullable=False)

# Carrega o usuário atual para o Flask-Login
@login_manager.user_loader
def load_user(user_id):
    return AdminUser.query.get(int(user_id))

# --- Lógica de formatação de data (sem alterações) ---
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

# --- 3. ROTAS PÚBLICAS (PAINEL PRINCIPAL) ---

@app.route('/', methods=['GET', 'POST'])
def dashboard():
    # A lista de vendedores a ser exibida agora vem da tabela VisibleSeller
    visible_sellers_from_db = VisibleSeller.query.order_by(VisibleSeller.seller_name).all()
    vendedores_para_exibir = [s.seller_name for s in visible_sellers_from_db]

    # --- Lógica do Relatório Diário ---
    data_atual = datetime.now().date()
    relatorio_final = []
    total_vl = 0
    total_cc = 0

    try:
        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
        with engine.connect() as conexao:
            query = text("SELECT vendedor, situacao, metodo_pagamento, dt_pedido FROM vendas")
            df_total = pd.read_sql(query, conexao)
        
        df_total['dt_pedido'] = pd.to_datetime(df_total.get('dt_pedido'), errors='coerce')
        df_hoje = df_total[df_total['dt_pedido'].dt.date == data_atual].copy()
        vendas_liquidas_hoje = df_hoje[df_hoje['situacao'].isin(['EM ANDAMENTO', 'INSTALADA'])].copy()

        for vendedor in vendedores_para_exibir:
            vendas_do_vendedor = vendas_liquidas_hoje[vendas_liquidas_hoje['vendedor'] == vendedor]
            vl = len(vendas_do_vendedor)
            cc = len(vendas_do_vendedor[vendas_do_vendedor['metodo_pagamento'] == 'CARTÃO DE CRÉDITO'])
            cc_percent = (cc / vl * 100) if vl > 0 else 0
            relatorio_final.append({'vendedor': vendedor, 'vl': vl, 'cc': cc, 'cc_percent': cc_percent})
            total_vl += vl
            total_cc += cc

    except Exception as e:
        print(f"Erro ao gerar relatório diário: {e}")
        relatorio_final = [{'vendedor': v, 'vl': 0, 'cc': 0, 'cc_percent': 0} for v in vendedores_para_exibir]
    
    total_cc_percent = (total_cc / total_vl * 100) if total_vl > 0 else 0

    # --- Lógica da Busca de Vendas (sem alterações) ---
    resultados = None
    resumo_status = None
    nome_pesquisado = None

    if request.method == 'POST':
        nome_vendedor = request.form.get('nome_vendedor', '').strip()
        nome_pesquisado = nome_vendedor
        if nome_vendedor:
            try:
                engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
                with engine.connect() as conexao:
                    query_str = """
                        SELECT cliente, vendedor, os, situacao, dt_pedido, periodo, dt_inst
                        FROM vendas WHERE UPPER(vendedor) = :nome_vendedor_upper
                    """
                    query = text(query_str)
                    df_resultados = pd.read_sql(query, conexao, params={"nome_vendedor_upper": nome_vendedor.upper()})

                ano_mes_atual = datetime.now().strftime('%Y-%m')
                df_resultados['dt_pedido'] = pd.to_datetime(df_resultados.get('dt_pedido'), errors='coerce')
                df_resultados['dt_inst'] = pd.to_datetime(df_resultados.get('dt_inst'), errors='coerce')
                df_filtrado = df_resultados[
                    (df_resultados['dt_pedido'].dt.strftime('%Y-%m') == ano_mes_atual) |
                    (df_resultados['dt_inst'].dt.strftime('%Y-%m') == ano_mes_atual)
                ].copy()
                
                resultados = df_filtrado.to_dict('records')
                resumo_status = Counter([venda['situacao'] for venda in resultados]) if resultados else None
            except Exception as e:
                print(f"Erro ao buscar vendas por vendedor: {e}")
                resultados = []

    return render_template(
        'dashboard.html', relatorio=relatorio_final, total_vl=total_vl, total_cc=total_cc,
        total_cc_percent=total_cc_percent, vendas=resultados, nome_pesquisado=nome_pesquisado,
        resumo=resumo_status
    )

# --- 4. ROTAS DE ADMINISTRAÇÃO ---

@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if current_user.is_authenticated:
        return redirect(url_for('admin_config'))
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user = AdminUser.query.filter_by(username=username).first()
        
        if user and user.check_password(password):
            login_user(user)
            return redirect(url_for('admin_config'))
        else:
            flash('Usuário ou senha inválidos.')
            
    return render_template('admin_login.html')

@app.route('/admin/logout')
@login_required
def admin_logout():
    logout_user()
    return redirect(url_for('admin_login'))

@app.route('/admin/config', methods=['GET', 'POST'])
@login_required
def admin_config():
    # Busca todos os vendedores únicos da tabela 'vendas'
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    with engine.connect() as connection:
        result = connection.execute(text("SELECT DISTINCT vendedor FROM vendas ORDER BY vendedor"))
        all_sellers = [row[0] for row in result]
    
    if request.method == 'POST':
        # Pega a lista de vendedores que foram marcados no formulário
        selected_sellers = request.form.getlist('sellers')
        
        # Apaga a configuração antiga
        VisibleSeller.query.delete()
        
        # Adiciona a nova configuração
        for seller_name in selected_sellers:
            new_visible_seller = VisibleSeller(seller_name=seller_name)
            db.session.add(new_visible_seller)
        
        db.session.commit()
        flash('Lista de vendedores visíveis atualizada com sucesso!')
        return redirect(url_for('admin_config'))

    # Para o método GET, apenas exibe a página
    visible_sellers = [s.seller_name for s in VisibleSeller.query.all()]
    return render_template('admin_config.html', all_sellers=all_sellers, visible_sellers=visible_sellers)


# --- 5. COMANDOS PARA GERENCIAR A APLICAÇÃO ---
@app.cli.command("create-db")
def create_db():
    """Cria as tabelas do banco de dados."""
    db.create_all()
    print("Banco de dados criado.")

@app.cli.command("create-admin")
def create_admin():
    """Cria um novo usuário administrador."""
    import click
    username = click.prompt("Digite o nome de usuário do admin")
    password = click.prompt("Digite a senha", hide_input=True, confirmation_prompt=True)
    
    existing_user = AdminUser.query.filter_by(username=username).first()
    if existing_user:
        print(f"Usuário '{username}' já existe.")
        return
        
    new_admin = AdminUser(username=username)
    new_admin.set_password(password)
    db.session.add(new_admin)
    db.session.commit()
    print(f"Administrador '{username}' criado com sucesso.")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)