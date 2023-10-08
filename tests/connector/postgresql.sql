-- Create the first database
CREATE DATABASE database1;
\c database1;

-- Create a table in the first database
CREATE TABLE table1 (
    id serial PRIMARY KEY,
    name varchar(255) NOT NULL,
    age integer,
    salary numeric(10, 2),
    is_active boolean DEFAULT true
);

-- Insert sample data into the first table
INSERT INTO table1 (name, age, salary, is_active) VALUES
    ('John Doe', 30, 50000.50, true),
    ('Jane Smith', 25, 60000.75, true),
    ('Bob Johnson', 40, 75000.00, false);

-- Create the second database
CREATE DATABASE database2;
\c database2;

-- Create a table in the second database
CREATE TABLE table2 (
    product_id serial PRIMARY KEY,
    product_name varchar(100) NOT NULL,
    price numeric(8, 2),
    purchase_date date
);

-- Insert sample data into the second table
INSERT INTO table2 (product_name, price, purchase_date) VALUES
    ('Widget A', 19.99, '2023-01-15'),
    ('Gadget B', 49.99, '2023-02-20'),
    ('Thingamajig C', 9.95, '2023-03-10');
