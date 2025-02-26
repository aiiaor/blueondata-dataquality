-- Step 1: Create Table
CREATE TABLE ecommerce_orders (
    order_id INT,
    customer VARCHAR(100),
    order_date VARCHAR(50),  -- Intentionally using VARCHAR for inconsistent formats
    product VARCHAR(100),
    quantity INT NULL,  -- Some missing values
    price DECIMAL(10,2) NULL  -- Some missing values
);

-- Step 2: Insert Sample Data
INSERT INTO ecommerce_orders (order_id, customer, order_date, product, quantity, price) VALUES
    (1001, 'alice', '2021/01/15', 'Laptop', 1, 1200.00),
    (1002, ' Bob ', '15-01-2021', 'smartphone', 2, 800.00),
    (1003, 'charlie', 'January 15, 2021', 'Tablet', NULL, 400.00),
    (1002, 'Bob', '15/01/2021', 'Smartphone', 2, 800.00),  -- Duplicate order_id
    (1004, 'David', '2021.01.16', 'laptop ', 1, 1200.00),
    (1005, '  eve', '16-01-2021', 'Tablet', 3, 400.00),
    (1006, 'Frank', '01/17/2021', 'Smart Phone', 2, NULL),  -- Missing price
    (1007, 'Grace', '2021-01-17', 'Laptop', 1, 1200.00),
    (1008, 'Hank', '17-01-2021', 'laptop', NULL, 1200.00),
    (1006, 'frank', '2021/01/17', 'Smartphone', 2, 800.00),  -- Duplicate order_id
    (1009, 'Isabella', '2021/01/18', 'Tablet', 1, 400.00),
    (1010, 'Jack', '18-01-2021', 'Laptop', 2, 1200.00),
    (1011, 'Kevin ', 'January 18, 2021', 'laptop ', NULL, 1200.00),
    (1012, 'Lucy', '19/01/2021', 'Tablet', 3, 400.00),
    (1013, 'Mike', '2021.01.19', 'Smart Phone', 2, NULL),
    (1014, 'Nancy', '2021-01-20', 'Laptop', 1, 1200.00),
    (1015, 'Oliver', '2021/01/21', 'Tablet', 2, 400.00),
    (1016, 'Patricia', '21-01-2021', 'Laptop', NULL, 1200.00),
    (1017, 'Quinn', 'January 22, 2021', 'Smart Phone', 3, 800.00),
    (1018, 'Rachel', '23/01/2021', 'Tablet', 1, 400.00),
    (1019, 'Steve', '2021.01.24', 'Laptop', 2, NULL),
    (1020, 'Tom', '2021-01-25', 'Smart Phone', 1, 800.00),
    (1021, 'Uma', '2021/01/26', 'Tablet', 2, 400.00),
    (1022, 'Victor', '26-01-2021', 'Laptop', NULL, 1200.00),
    (1023, 'Wendy', 'January 27, 2021', 'Smart Phone', 3, NULL),
    (1024, 'Xavier', '28/01/2021', 'Tablet', 1, 400.00),
    (1025, 'Yasmine', '2021.01.29', 'Laptop', 2, 1200.00),
    (1026, 'Zack', '2021-01-30', 'Smart Phone', 1, 800.00);

-- Generate additional 75 random records (modify for real database engines)
INSERT INTO ecommerce_orders (order_id, customer, order_date, product, quantity, price)
SELECT
    1027 + s.n, -- Generating unique order IDs
    CASE WHEN s.n % 2 = 0 THEN 'Random Customer' ELSE 'Another User' END, -- Alternating names
    CASE 
        WHEN s.n % 3 = 0 THEN '2021/02/' || (s.n % 28 + 1) 
        WHEN s.n % 3 = 1 THEN (s.n % 28 + 1) || '-02-2021' 
        ELSE 'February ' || (s.n % 28 + 1) || ', 2021' 
    END, -- Generating inconsistent date formats
    CASE 
        WHEN s.n % 4 = 0 THEN 'Smartphone' 
        WHEN s.n % 4 = 1 THEN 'Laptop' 
        WHEN s.n % 4 = 2 THEN 'Tablet' 
        ELSE 'Smart Phone' 
    END, -- Generating different product names
    CASE WHEN s.n % 5 = 0 THEN NULL ELSE (s.n % 5 + 1) END, -- Some missing quantities
    CASE WHEN s.n % 7 = 0 THEN NULL ELSE ((s.n % 3 + 1) * 400) END -- Some missing prices
FROM generate_series(1, 75) AS s(n); -- Generating 75 more records dynamically
