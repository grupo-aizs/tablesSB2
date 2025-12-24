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
            
        t_vatu_f = float(t_vatu) if t_vatu else 0.0
        p_vatu_f = float(p_vatu) if p_vatu else 0.0
        t_cm_f = float(t_cm) if t_cm else 0.0
        p_cm_f = float(p_cm) if p_cm else 0.0

        diff_vatu = abs(t_vatu_f - p_vatu_f) > 0.001
        diff_cm = abs(t_cm_f - p_cm_f) > 0.001
        has_diff = diff_vatu or diff_cm
        
        full_data.append({
            "filial": t_filial,
            "cod": t_cod,
            "local": t_local,
            "t_vatu": f"{t_vatu_f:.6f}",
            "t_cm": f"{t_cm_f:.6f}",
            "p_vatu": f"{p_vatu_f:.6f}",
            "p_cm": f"{p_cm_f:.6f}",
            "diff_vatu": diff_vatu,
            "diff_cm": diff_cm,
            "has_diff": has_diff
        })

    print(f"--- [CACHE SET] Dados processados em {time.time() - start_t:.2f}s ---")
    CACHE_DATA = full_data
    CACHE_TIMESTAMP = time.strftime("%H:%M:%S")
    return full_data

@app.route("/")
def index():
    page = request.args.get('page', 1, type=int)
    filter_type = request.args.get('filter', 'all')
    force_reload = request.args.get('reload', '0') == '1'
    per_page = 100

    # Pega dados do cache (ou carrega se necessário/forçado)
    full_data = get_cached_data(force_reload=force_reload)
    
    # Aplica filtros na lista em memória (muito rápido)
    if filter_type == 'diff':
        filtered_data = [item for item in full_data if item['has_diff']]
    elif filter_type == 'equal':
        filtered_data = [item for item in full_data if not item['has_diff']]
    else:
        filtered_data = full_data

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

import csv
from flask import Response, stream_with_context

@app.route("/export_excel")
def export_excel():
    # 1. Obter dados
    data = get_cached_data() 
    if not data:
        data = []

    # 2. Gerador para Streaming (Baixa instantaneamente e não trava o servidor)
    def generate():
        # BOM para Excel reconhecer acentos
        yield '\ufeff' 
        
        # Cria um buffer de string em memória para o CSV writer
        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        # Cabeçalhos
        headers = ["FILIAL", "PRODUTO", "LOCAL", "TESTE_VATU1", "TESTE_CM1", "PROD_VATU1", "PROD_CM1", "DIVERGENCIA"]
        writer.writerow(headers)
        
        # Envia headers
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        # Dados
        for item in data:
            # Converter float
            try:
                t_vatu = float(item['t_vatu'])
                t_cm = float(item['t_cm'])
                p_vatu = float(item['p_vatu'])
                p_cm = float(item['p_cm'])
            except:
                t_vatu = t_cm = p_vatu = p_cm = 0.0

            # Formata floats com virgula para Excel PT-BR
            def fmt(v):
                return f"{v:.6f}".replace('.', ',')

            # Checa divergência
            # (Adicionamos uma coluna explícita já que CSV não tem cor)
            div_text = "SIM" if (item['diff_vatu'] or item['diff_cm']) else "NAO"

            writer.writerow([
                item['filial'],
                item['cod'],
                item['local'],
                fmt(t_vatu),
                fmt(t_cm),
                fmt(p_vatu),
                fmt(p_cm),
                div_text
            ])
            
            # Envia o chunk processado
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    # 3. Retorna resposta Streamada
    filename = f"comparacao_sb2_{time.strftime('%Y%m%d_%H%M')}.csv"
    
    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=9901)
