# Dataset example

Dataset generated with ChatGPT:

Illustrate a relational database example with tables connected by primary keys,
minimum 5 relations and with which one could do statistical analysis with if
populated with relevant data.

Let's consider a database related to an e-commerce platform with tables for
`Customers`, `Orders`, `Products`, `OrderDetails`, and `Categories`. This example
includes fields that could be used for statistical analysis.

1. **Customers Table:**
   - Fields: `CustomerID` (Primary Key), `FirstName`, `LastName`, `Email`, `RegistrationDate`

   ```plaintext
   | CustomerID | FirstName | LastName | Email                    | RegistrationDate |
   |------------|-----------|----------|--------------------------|-------------------|
   | 1001       | Alice     | Johnson  | alice.j@example.com      | 2022-01-15        |
   | 1002       | Bob       | Smith    | bob.smith@example.com    | 2022-02-20        |
   | 1003       | Charlie   | Brown    | charlie.b@example.com    | 2022-03-10        |
   ```

2. **Orders Table:**
   - Fields: `OrderID` (Primary Key), `CustomerID` (Foreign Key), `OrderDate`, `TotalAmount`

   ```plaintext
   | OrderID | CustomerID | OrderDate  | TotalAmount |
   |---------|------------|------------|-------------|
   | 2001    | 1001       | 2022-04-01 | 150.00      |
   | 2002    | 1002       | 2022-04-05 | 200.00      |
   | 2003    | 1003       | 2022-04-10 | 120.00      |
   ```

3. **Products Table:**
   - Fields: `ProductID` (Primary Key), `ProductName`, `CategoryID` (Foreign Key), `UnitPrice`

   ```plaintext
   | ProductID | ProductName         | CategoryID | UnitPrice |
   |-----------|---------------------|------------|-----------|
   | 3001      | Laptop              | 4001       | 800.00    |
   | 3002      | Smartphone          | 4002       | 400.00    |
   | 3003      | Headphones          | 4003       | 50.00     |
   ```

4. **OrderDetails Table:**
   - Fields: `DetailID` (Primary Key), `OrderID` (Foreign Key), `ProductID` (Foreign Key), `Quantity`

   ```plaintext
   | DetailID | OrderID | ProductID  | Quantity |
   |----------|---------|------------|----------|
   | 4001     | 2001    | 3001       | 1        |
   | 4002     | 2001    | 3002       | 2        |
   | 4003     | 2002    | 3003       | 3        |
   ```

5. **Categories Table:**
   - Fields: `CategoryID` (Primary Key), `CategoryName`

   ```plaintext
   | CategoryID | CategoryName |
   |------------|--------------|
   | 4001       | Electronics  |
   | 4002       | Mobile       |
   | 4003       | Audio        |
   ```

In this example:

- In the `Customers` table, `CustomerID` is the primary key.
- In the `Orders` table, `OrderID` is the primary key. `CustomerID` is a foreign key.
- In the `Products` table, `ProductID` is the primary key. `CategoryID` is a foreign key.
- In the `OrderDetails` table, `DetailID` is the primary key. Both `OrderID` and
  `ProductID` are foreign keys, establishing relationships with the `Orders` and
  `Products` tables, respectively.
- In the `Categories` table, `CategoryID` is the primary key.

This database structure enables statistical analysis related to customer orders,
such as calculating total revenue, average order value, popular products, and sales
by category. It can support queries and analysis to gain insights into customer
behavior and product performance.
