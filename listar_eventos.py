"""
Utilitário de Leitura de Banco de Dados v1.0
Justificativa: Visualizar os dados do SQLite de forma amigável no terminal mobile, 
evitando a quebra de linhas horizontal do comando sqlite3 padrão.
"""
import sqlite3
import os
import logging

# Configuração de log para depuração
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def visualizar_banco():
    db_path = "./data/mg_events.db"
    
    if not os.path.exists(db_path):
        logging.error(f"Banco de dados não encontrado no caminho: {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Busca as principais colunas para exibição
        cursor.execute("SELECT titulo, data_evento, cidade, local, fonte, preco_base, id_unico FROM eventos")
        eventos = cursor.fetchall()
        
        print("\n" + "="*50)
        print(f" 📊 TOTAL DE EVENTOS CADASTRADOS: {len(eventos)}")
        print("="*50 + "\n")
        
        if not eventos:
            print("O banco de dados está vazio. Nenhuma informação para exibir.")
            return

        for i, ev in enumerate(eventos, 1):
            titulo, data, cidade, local, fonte, preco, id_unico = ev
            print(f"🟢 EVENTO [{i}]")
            print(f"   Título : {titulo}")
            print(f"   Data   : {data}")
            print(f"   Local  : {cidade} - {local}")
            print(f"   Preço  : R$ {preco:.2f}")
            print(f"   Fonte  : {fonte}")
            print(f"   Hash ID: {id_unico[:8]}...") # Exibe só o começo do hash
            print("-" * 50)
            
    except sqlite3.OperationalError as e:
        logging.error(f"Erro operacional no banco (Tabela não existe?): {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    visualizar_banco()
