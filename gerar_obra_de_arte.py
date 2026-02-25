import sqlite3
import json
import os

def exportar_para_json():
    db_path = './data/mg_events.db'
    output_path = 'export_eventos.json'
    
    if not os.path.exists(db_path):
        print("❌ Banco de dados não encontrado!")
        return

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Permite acessar colunas pelo nome
        cursor = conn.cursor()
        
        print("🔍 Lendo eventos do banco...")
        cursor.execute("""
            SELECT fonte, titulo, local, categoria, url_evento, 
                   datetime(data_evento) as data, 
                   datetime(detectado_em, 'localtime') as capturado_em
            FROM eventos 
            ORDER BY fonte ASC, titulo ASC
        """)
        
        rows = cursor.fetchall()
        eventos = [dict(row) for row in rows]
        
        # Estrutura o JSON com metadados
        resultado_final = {
            "projeto": "MG-Event-Hub",
            "autor": "Felipe Moreira Franco",
            "total_eventos": len(eventos),
            "ultima_atualizacao": eventos[0]['capturado_em'] if eventos else "N/A",
            "eventos": eventos
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(resultado_final, f, ensure_ascii=False, indent=4)
            
        print(f"✅ OBRA DE ARTE CONCLUÍDA: {output_path}")
        print(f"📊 Total de eventos exportados: {len(eventos)}")
        
    except Exception as e:
        print(f"❌ Erro na exportação: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    exportar_para_json()
