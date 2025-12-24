from flask import Flask, render_template, jsonify
import pyodbc

import os
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from flask import send_file

app = Flask(__name__)

FILIAL = "09ALFA01"

TESTE_SQL = {
    "server": os.environ.get("MSSQL_HOST_TST", "192.168.0.246"),
    "database": os.environ.get("MSSQL_DB_TST", "protheus12_producao"),
    "user": os.environ.get("MSSQL_USER_TST", "sa"),
    "password": os.environ.get("MSSQL_PASSWORD_TST", "mYK#LTtiA2lu"),
    "port": int(os.environ.get("MSSQL_PORT_TST", 1433)),
}

# Configuração de Produção - Pode ser sobrescrita via ENV
PROD_SQL = {
    "server": os.environ.get("MSSQL_HOST_PROD", "192.168.0.243"),
    "database": os.environ.get("MSSQL_DB_PROD", "protheus12_producao"),
    "user": os.environ.get("MSSQL_USER_PROD", "consulta2"),
    "password": os.environ.get("MSSQL_PASSWORD_PROD", "consulta2"),
    "port": int(os.environ.get("MSSQL_PORT_PROD", 1433)),
}

def format_br(value):
    """Formata float para padrão BR (1.234,567890) com 6 casas"""
    if value is None:
        return "0,000000"
    # Formata como US (com virgula de milhar e ponto decimal)
    us_fmt = "{:,.6f}".format(value)
    # Inverte os caracteres
    return us_fmt.replace(",", "X").replace(".", ",").replace("X", ".")

app.jinja_env.filters['format_br'] = format_br

def pick_driver():
    drivers = pyodbc.drivers()
    for preferred in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"):
        if preferred in drivers:
            return preferred
    if not drivers:
        raise RuntimeError("Nenhum ODBC Driver para SQL Server encontrado.")
    return drivers[-1]

DRIVER = pick_driver()

def connect_sql(cfg: dict):
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={cfg['server']},{cfg['port']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['user']};"
        f"PWD={cfg['password']};"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str, timeout=10)

def _trim(v):
    return v.rstrip() if isinstance(v, str) else v

def get_produtos_teste():
    sql = """
        SELECT B2_FILIAL, B2_COD, B2_LOCAL, B2_VATU1, B2_CM1
          FROM SB2010
         WHERE B2_FILIAL = ?
         AND D_E_L_E_T_ = ''
         AND B2_COD <> ''
         AND (B2_VATU1 <> 0 OR B2_CM1 <> 0)
         ORDER BY B2_COD, B2_LOCAL
    """
    with connect_sql(TESTE_SQL) as conn:
        cur = conn.cursor()
        cur.execute(sql, (FILIAL,))
        rows = cur.fetchall()

        # [(filial, cod, local, vatu1, cm1), ...]
        return [(_trim(r.B2_FILIAL), _trim(r.B2_COD), _trim(r.B2_LOCAL), r.B2_VATU1, r.B2_CM1) for r in rows]

def sync_to_prod(produtos):
    # Atualiza só se existir e só se mudou
    sql_update = """
        UPDATE p
           SET p.B2_VATU1 = ?,
               p.B2_CM1   = ?
          FROM SB2010 p
         WHERE p.B2_FILIAL = ?
           AND p.B2_COD    = ?
           AND ISNULL(p.D_E_L_E_T_,'') = ''
           AND (
                ISNULL(p.B2_VATU1, 0) <> ISNULL(?, 0)
             OR ISNULL(p.B2_CM1,   0) <> ISNULL(?, 0)
           )
    """

    sql_exists = """
        SELECT 1
          FROM SB2010
         WHERE B2_FILIAL = ?
           AND B2_COD    = ?
           AND ISNULL(D_E_L_E_T_,'') = ''
    """

    atualizados = 0
    nao_existem = 0

    with connect_sql(PROD_SQL) as conn:
        conn.autocommit = False
        cur = conn.cursor()

        for filial, cod, vatu1, cm1 in produtos:
            filial_key = _trim(filial)
            cod_key = _trim(cod)

            # sanity: por segurança, não deixa sincronizar outra filial sem querer
            if filial_key != FILIAL:
                continue

            cur.execute(sql_exists, (filial_key, cod_key))
            if cur.fetchone() is None:
                nao_existem += 1
                continue

            # params: new_vatu1, new_cm1, filial, cod, cmp_vatu1, cmp_cm1
            cur.execute(sql_update, (vatu1, cm1, filial_key, cod_key, vatu1, cm1))
            if cur.rowcount > 0:
                atualizados += 1

        conn.commit()

    return atualizados, nao_existem, len(produtos)

from flask import Flask, render_template, jsonify, request
import math

# ... (rest of imports)

import time

# Cache Global simples
CACHE_DATA = None
CACHE_TIMESTAMP = None

def get_cached_data(force_reload=False):
    global CACHE_DATA, CACHE_TIMESTAMP
    
    # Se já tem dados e não forçado, retorna cache
    if CACHE_DATA is not None and not force_reload:
        return CACHE_DATA

    print("--- [CACHE MISS] Carregando dados do SQL... ---")
    start_t = time.time()
    
    # 1. Busca dados
    test_data = get_produtos_teste()
    raw_prod = get_produtos_prod()
    prod_dict = {(r[1], r[2]): (r[3], r[4]) for r in raw_prod}
    
    # 2. Processa em memória
    full_data = []
    for t_filial, t_cod, t_local, t_vatu, t_cm in test_data:
        p_val = prod_dict.get((t_cod, t_local))
        
        if p_val:
            p_vatu, p_cm = p_val
        else:
            p_vatu, p_cm = 0.0, 0.0
            
        t_vatu_f = round(float(t_vatu) if t_vatu else 0.0, 6)
        p_vatu_f = round(float(p_vatu) if p_vatu else 0.0, 6)
        t_cm_f = round(float(t_cm) if t_cm else 0.0, 6)
        p_cm_f = round(float(p_cm) if p_cm else 0.0, 6)

        # Agora a comparação pode ser exata (ou com epsilon muito baixo),
        # pois já arredondamos para o que é visível.
        diff_vatu = abs(t_vatu_f - p_vatu_f) > 0.000001
        diff_cm = abs(t_cm_f - p_cm_f) > 0.000001
        has_diff = diff_vatu or diff_cm
        
        full_data.append({
            "filial": t_filial,
            "cod": t_cod,
            "local": t_local,
            "t_vatu": t_vatu_f,
            "t_cm": t_cm_f,
            "p_vatu": p_vatu_f,
            "p_cm": p_cm_f,
            "diff_vatu": diff_vatu,
            "diff_cm": diff_cm,
            "has_diff": has_diff
        })

    print(f"--- [CACHE SET] Dados processados em {time.time() - start_t:.2f}s ---")
    CACHE_DATA = full_data
    CACHE_TIMESTAMP = time.strftime("%H:%M:%S")
    return full_data

def apply_filter(data, filter_type):
    if filter_type == 'diff':
        return [item for item in data if item['has_diff']]
    elif filter_type == 'equal':
        return [item for item in data if not item['has_diff']]
    return data

@app.route("/")
def index():
    page = request.args.get('page', 1, type=int)
    filter_type = request.args.get('filter', 'all')
    force_reload = request.args.get('reload', '0') == '1'
    per_page = 100

    # Pega dados do cache (ou carrega se necessário/forçado)
    full_data = get_cached_data(force_reload=force_reload)
    
    # Aplica filtros helper
    filtered_data = apply_filter(full_data, filter_type)

    # Calculo de Totais (Baseado no filtro atual)
    totals = {
        't_vatu': sum(item['t_vatu'] for item in filtered_data),
        't_cm': sum(item['t_cm'] for item in filtered_data),
        'p_vatu': sum(item['p_vatu'] for item in filtered_data),
        'p_cm': sum(item['p_cm'] for item in filtered_data),
    }

    # Paginação
    total_items = len(filtered_data)
    total_pages = math.ceil(total_items / per_page) if total_items > 0 else 1
    
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    
    start = (page - 1) * per_page
    end = start + per_page
    
    paginated_data = filtered_data[start:end]

    return render_template(
        "index.html",
        comparison_data=paginated_data,
        filial=FILIAL,
        last_update=CACHE_TIMESTAMP,
        
        # Stats
        total_items=total_items, # Total filtrado
        total_full=len(full_data), # Total absoluto
        totals=totals, # Somas
        
        # Pagination & Filter
        page=page,
        total_pages=total_pages,
        current_filter=filter_type
    )

def get_produtos_prod():
    sql = """
        SELECT B2_FILIAL, B2_COD, B2_LOCAL, B2_VATU1, B2_CM1
          FROM SB2010
         WHERE B2_FILIAL = ?
         AND D_E_L_E_T_ = ''
         AND B2_COD <> ''
         -- Mantemos o filtro para não trazer lixo, 
         -- mas se o produto existir e estiver zerado, 
         -- cairá no 'else' do loop acima assumindo 0.0, o que está ok.
         AND (B2_VATU1 <> 0 OR B2_CM1 <> 0)
         ORDER BY B2_COD, B2_LOCAL
    """
    with connect_sql(PROD_SQL) as conn:
        cur = conn.cursor()
        cur.execute(sql, (FILIAL,))
        rows = cur.fetchall()
        return [(_trim(r.B2_FILIAL), _trim(r.B2_COD), _trim(r.B2_LOCAL), r.B2_VATU1, r.B2_CM1) for r in rows]

# Endpoint sync removido para este modo de comparação

from openpyxl.cell import WriteOnlyCell

import xlsxwriter

@app.route("/export_excel")
def export_excel():
    # 0. Obter dados e filtro
    filter_type = request.args.get('filter', 'all')
    data = get_cached_data() 
    if not data:
        data = []
    
    # 1. Aplicar o MEIO FILTRO que está na tela
    data = apply_filter(data, filter_type)

    # 2. Configurar XlsxWriter com Constant Memory (Baixo uso de RAM)
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'constant_memory': True, 'in_memory': False})
    worksheet = workbook.add_worksheet("Comparacao SB2")

    # Estilos
    header_fmt = workbook.add_format({
        'bold': True,
        'font_color': '#FFFFFF',
        'bg_color': '#0B1220',
        'align': 'center'
    })
    
    total_fmt = workbook.add_format({
        'bold': True,
        'bg_color': '#e1e1e1',
        'num_format': '0.000000',
        'top': 1
    })

    diff_fmt = workbook.add_format({'bold': True, 'font_color': '#FF0000', 'num_format': '0.000000'})
    normal_fmt = workbook.add_format({'num_format': '0.000000'})
    text_fmt = workbook.add_format({})

    # 3. Cabeçalhos
    headers = ["FILIAL", "PRODUTO", "LOCAL", "TESTE_VATU1", "TESTE_CM1", "PROD_VATU1", "PROD_CM1"]
    for col, h in enumerate(headers):
        worksheet.write(0, col, h, header_fmt)

    # Ajuste largura colunas (aprox)
    worksheet.set_column(0, 0, 10) # Filial
    worksheet.set_column(1, 1, 20) # Produto
    worksheet.set_column(2, 2, 10) # Local
    worksheet.set_column(3, 6, 15) # Valores

    # Acumuladores de Totais
    sum_t_vatu = 0.0
    sum_t_cm = 0.0
    sum_p_vatu = 0.0
    sum_p_cm = 0.0

    # 4. Loop de Dados
    row_idx = 0
    for i, item in enumerate(data, start=1):
        row_idx = i
        # Conversões
        # Conversões (Agora já são floats, mas garantindo default 0.0)
        t_vatu = item.get('t_vatu', 0.0)
        t_cm = item.get('t_cm', 0.0)
        p_vatu = item.get('p_vatu', 0.0)
        p_cm = item.get('p_cm', 0.0)
        
        # Soma
        sum_t_vatu += t_vatu
        sum_t_cm += t_cm
        sum_p_vatu += p_vatu
        sum_p_cm += p_cm

        # Escrever células
        worksheet.write(row_idx, 0, item['filial'], text_fmt)
        worksheet.write(row_idx, 1, item['cod'], text_fmt)
        worksheet.write(row_idx, 2, item['local'], text_fmt)
        
        worksheet.write(row_idx, 3, t_vatu, normal_fmt)
        worksheet.write(row_idx, 4, t_cm, normal_fmt)
        
        # Formatação Condicional na Linha
        fmt_vatu = diff_fmt if item['diff_vatu'] else normal_fmt
        fmt_cm = diff_fmt if item['diff_cm'] else normal_fmt
        
        worksheet.write(row_idx, 5, p_vatu, fmt_vatu)
        worksheet.write(row_idx, 6, p_cm, fmt_cm)

    # 5. Escrever Totais na última linha
    last_row = row_idx + 1
    worksheet.write(last_row, 0, "TOTAL GERAL", total_fmt)
    # Mesclar ou deixar vazio as colunas B e C
    worksheet.write(last_row, 1, "", total_fmt)
    worksheet.write(last_row, 2, "", total_fmt)
    
    worksheet.write(last_row, 3, sum_t_vatu, total_fmt)
    worksheet.write(last_row, 4, sum_t_cm, total_fmt)
    worksheet.write(last_row, 5, sum_p_vatu, total_fmt)
    worksheet.write(last_row, 6, sum_p_cm, total_fmt)

    # 6. Fechar e Enviar
    workbook.close()
    output.seek(0)
    
    filter_label = f"_{filter_type}" if filter_type != 'all' else ""
    filename = f"comparacao_sb2{filter_label}_{time.strftime('%Y%m%d_%H%M')}.xlsx"

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=filename
    )

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9901)
