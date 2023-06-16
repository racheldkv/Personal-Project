--DATA CLEANING
-- Check duplicate value in TransactionID
Select TransactionID, count(TransactionID)
From PorfolioProject..banktransactions
Group by TransactionID
Having count(TransactionID) > 1

-- Check duplicate value in CustGender
Select CustGender, count(CustGender)
From PorfolioProject..banktransactions
Group by CustGender

-- Delete missing data in CustGender
Delete
From PorfolioProject..banktransactions
Where CustGender is null
	Or CustGender = 'T'

-- Delete missing data in the rest columns
Delete
From PorfolioProject..banktransactions
Where CustomerID is null
	Or CustomerDOB is null
	Or CustLocation is null
	Or CustAccountBalance is null
	Or TransactionDate is null
	Or TransactionTime is null
	Or TransactionAmount_INR is null

-- Find outliers in CustomerDOB
Select Min(CustomerDOB), Max(CustomerDOB)
From PorfolioProject..banktransactions

-- Change CustomerDOB data type to datetime
ALTER TABLE PorfolioProject..banktransactions
ALTER COLUMN CustomerDOB datetime

-- I will drop TransactionTime because I do not know this time indicates for what days or hours or minutes
Alter Table PorfolioProject..banktransactions
Drop Column TransactionTime

-- I will just keep customers with DOB from 1920 to 2023
Delete
From PorfolioProject..banktransactions
Where CustomerDOB < '1920-01-01' or CustomerDOB > '2023-01-01'

ALTER TABLE PorfolioProject..banktransactions
ALTER COLUMN CustomerDOB date



-- RFM ANALYSIS:

-- Calculate R: Recency - number of days since last purchase; F: Frequency - transaction frequency ; M: Monetary - total spend (TransactionAmount_INR)
	-- Note: 0 days mean that a customer has done transaction recently one time by logic so I will convert 0 to 1

Drop Table if exists rfm
Create Table rfm
(
CustomerID nvarchar(255),
Recency numeric,
Frequency numeric,
Monetary numeric
)

Insert into rfm 
Select  CustomerID, 
		REPLACE(DATEDIFF(day, Min(TransactionDate), Max(TransactionDate)), '0', '1'),
		Count(CustomerID),
		avg(TransactionAmount_INR)
From PorfolioProject..banktransactions
Group by CustomerID

-- Use quintile to separate customer into 5 parts base on R, F, M
	-- Champions: new customers who buy frequently and spend the most, loyal, willing to spend generously, and are likely to make another purchase soon
	-- Loyal Customers: average spending customers but buy very often
	-- Potential Loyalist: new customers with recent transactions, average spending, and have made more than one purchase
	-- Recent Customers: customers buy most recently, cart value is low and don't buy often
	-- Promising: Recently customers, have high purchasing power but not often
	-- Customers Needing Attention: Customers who have a moderate purchase frequency and cart value, have not returned to buy recently
	-- About To Sleep: customers who have not purchased for a long time, previously purchased with low frequency and low shopping cart value
	-- At Risk: customers who have not returned for a long time and used to purchase very often with a fairly average shopping cart value
	-- Cannot Lose Them: Customers who have not come back for a long time and used to make regular purchases, with a great shopping cart value
	-- Hibernating: Customers who have not returned for a long time, weak purchasing power
	-- Lost: Customers who have not returned for a long time, purchase frequency and cart value are also low

Select	CustomerID, rfm_recency, rfm_frequency, rfm_monetary,
		concat(rfm_recency, rfm_frequency, rfm_monetary) AS rfm_cell,
CASE WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('555','554','544', '545', '454', '455', '445') 
																THEN 'Champions'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('543', '444', '435', '355', '354', '345', '344', '335') 
																THEN 'Loyal Customers'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('553', '551', '552', '541', '542', '533', '532', '531', '452', '451', '442', '441',
																'431', '453', '433', '432', '423', '353', '352', '351', '342', '341', '333', '323') 
																THEN 'Potential Loyalist'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('512', '511', '422', '421', '412', '411', '311') 
																THEN 'Recent Customers'
	 WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('525', '524', '523', '522', '521', '515', '514', '513', 
																'425', '424', '413', '414', '415', '315', '314', '313') 
																THEN 'Promising'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('535', '534', '443', '434', '343', '334', '325', '324') 
																THEN 'Customers Needing Attention'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('331', '321', '312', '221', '213') 
																THEN 'About To Sleep'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('255', '254', '245', '244', '253', '252', '243', '242', '235', '234', '225',
																'224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124') 
																THEN 'At Risk'
     WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('155', '154', '144', '214', '215', '115', '114', '113') 
																THEN 'Cannot Lose Them'
	 WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('332', '322', '231', '241', '251', '233', '232',  
																'223', '222', '132', '123', '122', '212', '211') 
																THEN 'Hibernating'
	 WHEN concat(rfm_recency, rfm_frequency, rfm_monetary) IN ('111', '112', '121', '131', '141', '151') 
																THEN 'Lost'
ELSE 'Other' 
END AS rfm_segment
From 
	(Select CustomerID,
			ntile(5) OVER ( ORDER BY Recency ) AS rfm_recency,
			ntile(5) OVER ( ORDER BY Frequency ) AS rfm_frequency,
			ntile(5) OVER ( ORDER BY Monetary ) AS rfm_monetary
	From rfm
	) rfm1
Order by rfm_cell DESC

-- I will use Power BI to visualize these data.