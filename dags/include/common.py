from datetime import datetime

# Configurações padrão reutilizadas pelas DAGs produtora e consumidora
default_args = {
    'owner': 'silas',
    'start_date': datetime(2013, 1, 1),
    'retries': 2,

    
}
