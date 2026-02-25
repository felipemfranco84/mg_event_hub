from selectolax.parser import HTMLParser
import os

def escanear(arquivo):
    if not os.path.exists(arquivo):
        print(f"Arquivo {arquivo} não encontrado.")
        return
        
    with open(arquivo, 'r', encoding='utf-8', errors='ignore') as f:
        html = f.read()
        
    tree = HTMLParser(html)
    print(f"\n" + "="*50)
    print(f" 🩻 RAIO-X: {arquivo}")
    print("="*50)
    
    # Procurando onde estão os links e os títulos (h1, h2, h3)
    elementos = tree.css('h2, h3, h4, article a, div.row a, div.item a')
    
    contador = 0
    for el in elementos:
        texto = el.text(strip=True)
        if len(texto) > 10: # Só pega textos que parecem títulos de eventos
            pai = el.parent
            classe_pai = pai.attributes.get('class', 'SEM-CLASSE') if pai else 'SEM-PAI'
            tag_pai = pai.tag if pai else ''
            
            classe_el = el.attributes.get('class', 'SEM-CLASSE')
            
            print(f"PAI [ <{tag_pai} class='{classe_pai}'> ] -> ALVO [ <{el.tag} class='{classe_el}'> {texto[:50]}... ]")
            contador += 1
            
        if contador >= 15: # 15 amostras são suficientes para eu mapear a arquitetura
            break

escanear('dump_palacioartesextractor.txt')
escanear('dump_diarioammextractor.txt')
