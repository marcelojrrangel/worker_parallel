'''
Autor: Marcelo José Rodrigues Rangel
Data: 22/04/2020
Objetivo: Cria arquivo de testes
'''

def main():
        
    with open("/home/rangel/projetos/tmp/teste_carga.txt","a") as file1:
        for linha in range(1,80875):
            file1.write(str(linha) + ";teste;bla bla" + str(linha) + "\n")
    print("Arquivo criado.")    
    
main()            