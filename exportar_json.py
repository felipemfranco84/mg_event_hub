import sqlite3, json, os
from datetime import datetime

def exportar():
    # Caminho absoluto para evitar erros de diretório
    BASE_DIR = "/home/felicruel/apps/mg_event_hub"
    DB_PATH = os.path.join(BASE_DIR, "data/mg_events.db")
    # Colocando na pasta static dentro de app
    STATIC_DIR = os.path.join(BASE_DIR, "app/static")
    
    os.makedirs(STATIC_DIR, exist_ok=True)
    OUT_PATH = os.path.join(STATIC_DIR, "export_eventos.json")

    try:
        if not os.path.exists(DB_PATH):
            print(f"❌ Banco não encontrado em: {DB_PATH}")
            return

        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT fonte, titulo, local, categoria, url_evento, datetime(data_evento) as data FROM eventos ORDER BY data_evento ASC")
        eventos = [dict(row) for row in cursor.fetchall()]
        
        payload = {"total": len(eventos), "eventos": eventos}
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            
        # Permissões totais para o grupo do servidor (www-data)
        os.chmod(OUT_PATH, 0o664)
        print(f"✅ JSON gravado com sucesso em: {OUT_PATH}")
    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    exportar()
