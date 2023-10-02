with 
    stg_clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    ),
    
    criar_chave as (
        select 
            row_number() over(order by id_cliente) as pk_cliente,
            *
        from stg_clientes
    )

select *
from criar_chave