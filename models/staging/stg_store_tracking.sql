select
    e.EmployeeID as src_employee_id,
    e.FirstName as first_name,
    e.LastName as last_name,
    e.PhoneNumber as phone_number,
    e.JobPosition as position,
    e.HiredOn as hired_on,
    e.Address as employee_address,
    
    t.StoreID as src_store_id,
    t.StoreName as store_name,
    t.CountryCode as country_code,
    t.PostCode as post_code,
    t.address as address

from {{ source('second_dataset', 'e3e70682-c209-4cac-a29f-6fbed82c07cd') }} t,
unnest(t.Employees) as e

union all

select
    e.EmployeeID as src_employee_id,
    e.FirstName as first_name,
    e.LastName as last_name,
    e.PhoneNumber as phone_number,
    e.JobPosition as position,
    e.HiredOn as hired_on,
    e.Address as employee_address,
    
    t.StoreID as src_store_id,
    t.StoreName as store_name,
    t.CountryCode as country_code,
    t.PostCode as post_code,
    t.address as address
from {{ source('second_dataset', 'f728b4fa-4248-4e3a-8a5d-2f346baa9455') }} t,
unnest(t.Employees) as e
