select
    e.EmployeeID as src_employee_id,
    i.ModelID as src_model_id,
    i.ReportDate as report_date,
    i.ReportingWarehouseID as warehouse_id,
    i.Quantity as quantity,
    t.CountryCode as country_code,
    t.StoreID as src_store_id,
    t.StoreName as store_name
from {{ source('second_dataset', 'e3e70682-c209-4cac-a29f-6fbed82c07cd') }} t,
unnest(t.Employees) as e,
unnest(t.Inventory) as i

union all

select
    e.EmployeeID as src_employee_id,
    i.ModelID as src_model_id,
    i.ReportDate as report_date,
    i.ReportingWarehouseID as warehouse_id,
    i.Quantity as quantity,
    t.CountryCode as country_code,
    t.StoreID as src_store_id,
    t.StoreName as store_name
from {{ source('second_dataset', 'f728b4fa-4248-4e3a-8a5d-2f346baa9455') }} t,
unnest(t.Employees) as e,
unnest(t.Inventory) as i