class Query :
    datamart_customers_orders = (
        """
        begin;
        
        delete from data_mart.customers_orders
        where order_purchase between '{from_date}' and '{up_to_date}';

        insert into data_mart.customers_orders
        with 
            basic_data as (
            select orders.order_id,
                        c.customer_id,
                        orders.order_status ,
                        orders.order_purchase_timestamp ,
                        c.customer_city ,
                        op.payment_type ,
                        oi.price ,
                        s.seller_id ,
                        s.seller_city,
                        p.product_id ,
                        p.product_category_name 
                from orders 
                    left join customers c on orders.customer_id  = c.customer_id 
                    left join order_payments op on orders.order_id = op.order_id 
                    left join order_items oi on orders.order_id = oi.order_id 
                    left join sellers s on oi.seller_id = s.seller_id 
                    left join products p on oi.product_id = p.product_id
                )	
                select 
                    order_purchase_timestamp::date order_purchase,
                    customer_id ,
                    customer_city,
                    seller_id,
                    seller_city,
                    product_id,
                    product_category_name,
                    order_status ,
                    payment_type,
                    sum(price) total_gmv,
                    count(order_id) total_order,
                    count(customer_id) total_customer
                from basic_data
                where order_purchase_timestamp between '{from_date}' and '{up_to_date}'
                group by 1,2,3,4,5,6,7,8,9;

        end;
        """

    )