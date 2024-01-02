--I. Ad-hoc tasks
--1. Tổng số lượng người mua và số lượng đơn hàng đã hoàn thành mỗi tháng ( Từ 1/2019-4/2022)
select FORMAT_DATE('%Y-%m', created_at) AS month_year, 
  count(distinct user_id) as total_user, 
  count(order_id) as total_order
from bigquery-public-data.thelook_ecommerce.orders
where status = 'Complete' 
  and created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
group by 1
order by 1;
/* Insight: Lượng khách hàng và đơn hàng có xu hướng tăng dần theo thời gian. Gần như toàn bộ các khách hàng đều chỉ đặt 1 đơn hàng mỗi tháng. 
-> Có thể cho thấy chiến lược tiếp thị, quảng cáo hoặc chiến dịch kích thích mua hàng đang mang lại kết quả tích cực và đưa vào được nhiều khách hàng mới.*/

--2. Giá trị đơn hàng trung bình và tổng số người dùng khác nhau mỗi tháng 
select FORMAT_DATE('%Y-%m', created_at) AS month_year, 
  count(distinct user_id) as distinct_users,
  sum(sale_price)/count(order_id) as average_order_value
from bigquery-public-data.thelook_ecommerce.order_items
where created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
group by 1
order by 1;
/*Insight: tổng số người dùng khác nhau mỗi tháng có xu hướng tăng nhanh nhưng giá trị đơn hàng trung bình không tăng mà chỉ giao động quanh mức 60. Có thể xem xét một số nguyên nhân sau:
- Do chiến lược giảm giá hoặc khuyến mại:  Có thể do doanh nghiệp áp dụng nhiều chương trình giảm giá hoặc khuyến mãi để thu hút người dùng mới. Điều này có thể dẫn đến việc giảm giá trị đơn hàng trung bình.
- Chất lượng sản phẩm không tăng: Nếu chất lượng sản phẩm hoặc dịch vụ không tăng, có thể khách hàng không có động lực để mua nhiều hơn hoặc chi trả nhiều hơn cho mỗi đơn hàng.
- Chính sách giá không thay đổi: Nếu doanh nghiệp không áp dụng các chính sách giá mới hoặc các chiến lược tăng giá, giá trị đơn hàng trung bình có thể giữ nguyên.
*/

--3. Nhóm khách hàng theo độ tuổi (Từ 1/2019-4/2022)
with youngest as
(select first_name, last_name, gender, age, 'youngest' as tag
from  bigquery-public-data.thelook_ecommerce.users
where created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
  and age in (select min(age) as age
              from  bigquery-public-data.thelook_ecommerce.users
              where created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
              group by gender)
order by 1,2),
oldest as
(select first_name, last_name, gender, age, 'oldest' as tag
from  bigquery-public-data.thelook_ecommerce.users
where created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
  and age in (select max(age) as age
              from  bigquery-public-data.thelook_ecommerce.users
              where created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
              group by gender)
order by 1,2),
age as
(select * from youngest
union all
select * from oldest)
--Số lượng khách hàng trẻ nhất và lớn tuổi nhất:
select tag, count(*) from age group by tag;
/*Insight: khách hàng trẻ nhất: 12 tuổi - 1111 người. Khách hàng lớn tuổi nhất: 70 tuổi - 1107 người.
-> Tệp khách hàng phân phối ở đa dạng độ tuổi;
-> Chiến lược tiếp thị hiệu quả, giữ chân được cả người trẻ và người lón tuổi
-> Có tiềm năng tăng trưởng trong tất cả các phân khúc độ tuổi */

--4. Top 5 sản phẩm mỗi tháng.
with top5 as
(select month_year, product_id, product_name, sales, cost, profit,
  dense_rank() over(partition by month_year order by profit) as rank_per_month
from
(select FORMAT_DATE('%Y-%m', b.created_at) AS month_year, 
  a.id as product_id,
  a.name as product_name,
  round(b.sale_price,2) as sales,
  round(a.cost,2) as cost,
  round(a.retail_price - a.cost,2) as profit
from bigquery-public-data.thelook_ecommerce.products as a
inner join bigquery-public-data.thelook_ecommerce.order_items as b
on a.id = b.product_id
where b.created_at between timestamp('2019-01-01') and timestamp('2022-04-30')
order by 1) a)
select * from top5 where rank_per_month <=5 order by month_year;

--5. Doanh thu tính đến thời điểm hiện tại trên mỗi danh mục
select * from bigquery-public-data.thelook_ecommerce.order_items;

select FORMAT_DATE('%Y-%m-%d', b.created_at) AS dates,
  a.category as product_category,
  round(sum(a.retail_price),2) as revenue
from bigquery-public-data.thelook_ecommerce.products as a
join bigquery-public-data.thelook_ecommerce.order_items as b
on a.id = b.product_id
where b.created_at between timestamp('2022-01-15') and timestamp('2022-04-15')
group by 1, 2
order by 1, 2;

--III. Tạo metric trước khi dựng dashboard
--1. Dataset
with rg as
(SELECT 
  Month, Product_category,TPV,
  LAG(TPV) OVER (PARTITION BY Product_category ORDER BY month) AS previous_month_revenue,
  CASE 
    WHEN LAG(TPV) OVER (PARTITION BY Product_category ORDER BY month) IS NOT NULL THEN 
      CONCAT(ROUND((TPV - LAG(TPV) OVER (PARTITION BY Product_category ORDER BY month)) / LAG(TPV) OVER (PARTITION BY Product_category ORDER BY month),3) * 100.0,'%')
    ELSE NULL
  END AS revenue_growth
FROM 
  (select FORMAT_DATE('%Y-%m', orders.created_at) AS Month,
    products.category as Product_category,
    cast(sum(order_items.sale_price) as float64) as TPV
  from bigquery-public-data.thelook_ecommerce.orders as orders
  join bigquery-public-data.thelook_ecommerce.order_items as order_items
    on orders.user_id = order_items.user_id
  join bigquery-public-data.thelook_ecommerce.products as products
    on order_items.product_id = products.id
  group by 1,2
  order by 1,2) a
ORDER BY Product_category, month),

og as
(SELECT 
  Month, Product_category,TPO,
  LAG(TPO) OVER (PARTITION BY Product_category ORDER BY month) AS previous_month_order,
  CASE 
    WHEN LAG(TPO) OVER (PARTITION BY Product_category ORDER BY month) IS NOT NULL THEN 
      CONCAT(ROUND((TPO - LAG(TPO) OVER (PARTITION BY Product_category ORDER BY month)) / LAG(TPO) OVER (PARTITION BY Product_category ORDER BY month),3) * 100.0,'%')
    ELSE NULL
  END AS order_growth
FROM 
  (select FORMAT_DATE('%Y-%m', orders.created_at) AS Month,
    products.category as Product_category,
    cast(count(order_items.product_id) as float64) as TPO
  from bigquery-public-data.thelook_ecommerce.orders as orders
  join bigquery-public-data.thelook_ecommerce.order_items as order_items
    on orders.user_id = order_items.user_id
  join bigquery-public-data.thelook_ecommerce.products as products
    on order_items.product_id = products.id
  group by 1,2
  order by 1,2) a
ORDER BY Product_category, month),

tab as
(select FORMAT_DATE('%Y-%m', orders.created_at) AS Month,
  extract(year from orders.created_at) as Year,
  products.category as Product_category,
  sum(order_items.sale_price) as TPV,
  count(order_items.product_id) as TPO,
  sum(products.cost) as total_cost,
  sum(order_items.sale_price) - sum(products.cost) as total_profit,
  (sum(order_items.sale_price) - sum(products.cost))/sum(products.cost) as profit_to_cost_ratio
from bigquery-public-data.thelook_ecommerce.orders as orders
join bigquery-public-data.thelook_ecommerce.order_items as order_items
  on orders.user_id = order_items.user_id
join bigquery-public-data.thelook_ecommerce.products as products
  on order_items.product_id = products.id
group by 1,2,3
order by 1,2,3)

select tab.Month, tab.Year, tab.Product_category,tab.TPV,tab.TPO,
  rg.revenue_growth, og.order_growth, 
  tab.total_cost, tab.total_profit, tab.profit_to_cost_ratio
from tab
  join rg on tab.Month = rg.Month and tab.Product_category = rg.Product_category
  join og on tab.Month = og.Month and tab.Product_category = og.Product_category
order by 1,2,3;

--2. Cohort Analyst
with convert as
(select orders.order_id, 
  orders.user_id,
  order_items.product_id,
  orders.created_at as date,
  orders.num_of_item as quantity,
  order_items.sale_price
from bigquery-public-data.thelook_ecommerce.orders as orders
join bigquery-public-data.thelook_ecommerce.order_items as order_items
  on orders.order_id = order_items.order_id
where orders.status = 'Complete'
  and orders.user_id is not null
  and orders.num_of_item > 0 
  and order_items.sale_price > 0),
main as
(select *
from
  (select *,
  row_number() over (partition by order_id, product_id, quantity order by date) as stt
  from convert
  order by date) as a
where stt = 1),

indexm as
(select user_id, amount, 
  FORMAT_DATE('%Y-%m', first_purchase_date) as cohort_date,
  date,
  (extract(year from date)-extract(year from first_purchase_date))*12 + (extract(month from date)-extract(month from first_purchase_date)) +1 as index
from
  (select user_id,
    sale_price*quantity as amount,
    min(date) over(partition by user_id) as first_purchase_date,
    date
    from main) a),
cohort as
(select cohort_date, index,
  count(distinct user_id) as cnt,
  sum(amount) as revenue
from indexm
group by 1,2),

--customer cohort
customer as
(select cohort_date,
  sum(case when index=1 then cnt else 0 end) as m1,
  sum(case when index=2 then cnt else 0 end) as m2,
  sum(case when index=3 then cnt else 0 end) as m3,
  sum(case when index=4 then cnt else 0 end) as m4
from cohort
group by cohort_date),

--retention cohort
retention as
(select cohort_date,
  concat(round(100.00*m1/m1),"%") as m1,
  concat(round(100.00*m2/m1),"%") as m2,
  concat(round(100.00*m3/m1),"%") as m3,
  concat(round(100.00*m4/m1),"%") as m4
from customer)

--churn cohort
select cohort_date,
  concat(100-round(100.00*m1/m1),"%") as m1,
  concat(100-round(100.00*m2/m1),"%") as m2,
  concat(100-round(100.00*m3/m1),"%") as m3,
  concat(100-round(100.00*m4/m1),"%") as m4
from customer
