import threading
import itertools


def main():
    ''' programa para processar/ler arquivos grandes usando threads '''

    class newThread(threading.Thread):
        def __init__(self, threadID, nome, arquivo, fatia):
            threading.Thread.__init__(self)
            self.threadID = threadID
            self.nome = nome
            self.fatia = fatia
            self.arquivo = arquivo
            
        def run(self):
            ''' executa a thread '''
            print ("Iniciando thread id:%i %s" % (self.threadID, self.name))
            worker(self.threadID, self.nome, self.arquivo, self.fatia)
            print ("Finalizando " + self.nome)
    
    def worker(_id, nome, arquivo, fatia):
        ''' Trabalhador '''
        salvar_arquivo = "/home/rangel/projetos/tmp/teste_processado.txt"        
        with open(arquivo,"r") as file1:
            for line in itertools.islice(file1, fatia[0], fatia[1]):                
                with open(salvar_arquivo,"a") as file_save:
                    file_save.write(line)
    
    def get_dados_processo(total_linhas):
        ''' Regra de 3 simples '''
        #Aqui segue o exemplo de 1 para 8000 (ou seja um trabalhador lê 8000 linhas por vez)
        linhasarq1 = 8000 #->linhas lidas por vez
        trabalhadores1 = 1 #-> número de trabalhadores        
        #É o número de trabalhadores que serão necessários para executar a tarefa
        trabalhadores = (total_linhas * trabalhadores1) / linhasarq1
        linhas_por_vez = (linhasarq1 / trabalhadores1) #Total que um trabalhador aguenta por vez
        #Para não sobrarem linhas orfans / não lidas, quando der valor quebrado
        if trabalhadores % 1 > 0:
            trabalhadores += 1
        return int(trabalhadores), int(linhas_por_vez)
    
    #Orquestração das chamadas das threads
    def processar():
        ler_arquivo = "/home/rangel/projetos/tmp/teste_carga.txt"    
        total_linhas = sum(1 for line in open(ler_arquivo))
        print ("Total linhas:",total_linhas)
        trabalhadores, linhas_por_vez = get_dados_processo(total_linhas)
        threads = []    
        fatia_de = 0
        fatia_ate = linhas_por_vez
        print ("Trabalhadores:", trabalhadores)
        print("Linhas por trabalhador:", linhas_por_vez)
        for i in range(0, trabalhadores):
            # Criando as threads
            fatia = [fatia_de,fatia_ate]
            thread = newThread(i, "T-" + str(i), ler_arquivo, fatia)
            thread.start()
            threads.append(thread)
            #print ("Fatia", i, " de:" , fatia[0], " até", fatia[1])
            fatia_de += linhas_por_vez 
            fatia_ate += linhas_por_vez 
            if fatia_ate > total_linhas:
                fatia_ate = fatia_de + (total_linhas - fatia_de)                    
        for t in threads:
            t.join()    
        print ("Fim do processamento")
    
    processar()
    
main()