import httpx
import os
import zipfile
from selectolax.parser import HTMLParser
from tqdm import tqdm

def executar_fluxo():
    portal_url = "https://www.diariomunicipal.com.br/amm-mg/"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print("🔍 Acedendo ao portal para localizar o PDF...")
    with httpx.Client(headers=headers, follow_redirects=True) as client:
        resp = client.get(portal_url)
        tree = HTMLParser(resp.text)
        pdf_path = tree.css_first("input#urlPdf").attributes.get("value")
        
        if not pdf_path:
            print("❌ Não foi possível encontrar o PDF no portal.")
            return

        filename = pdf_path.split("/")[-1]
        zip_name = "pacote_diarios.zip"

        print(f"📥 Baixando: {filename}")
        # Download em modo Streaming para poupar RAM
        with client.stream("GET", pdf_path) as r:
            with open(filename, "wb") as f:
                for chunk in r.iter_bytes():
                    f.write(chunk)
        
        print(f"📦 Compactando em {zip_name}...")
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(filename)
            # Se quiser zipar todos os PDFs que já estão na pasta:
            # for file in os.listdir('.'):
            #     if file.endswith('.pdf'): zipf.write(file)
        
        # Opcional: Remover o PDF original após zipar para liberar espaço em disco
        # os.remove(filename)
        
        print(f"🚀 Sucesso! Ficheiro {zip_name} pronto para download.")

if __name__ == "__main__":
    executar_fluxo()
