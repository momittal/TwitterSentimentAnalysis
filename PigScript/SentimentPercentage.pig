-- 1. Load File

Sentiments = LOAD '/sentiment/part-r-00000' USING PigStorage('\t') AS (userName : chararray, sentiment: int);

-- 2. Count Number of records

Sentiments_Group = GROUP Sentiments ALL;

Sentiments_Count = FOREACH Sentiments_Group GENERATE COUNT(Sentiments) AS total;

-- 3. Get Positive, Negative and Neutral records
Pos = FILTER Sentiments BY sentiment > 0;
Neg = FILTER Sentiments BY sentiment < 0;
Neut = FILTER Sentiments BY sentiment == 0;


-- 4. Count Number of Positive Records

Pos_Group = GROUP Pos ALL;
Neg_Group = GROUP Neg ALL;
Neut_Group = GROUP Neut ALL;

Pos_Count = FOREACH Pos_Group GENERATE COUNT(Pos) AS pos_total;
Neg_Count = FOREACH Neg_Group GENERATE COUNT(Neg) AS neg_total;
Neut_Count = FOREACH Neut_Group GENERATE COUNT(Neut) AS neut_total;

-- 5. Calculate Percentage of Positve Record
PercJoin = CROSS Pos_Count,Neg_Count, Neut_Count, Sentiments_Count;

Perc = FOREACH PercJoin GENERATE (float)($0 * 100) / $3 AS posPerc, (float)($1 * 100) / $3 AS negPerc, (float)($2 * 100) / $3 AS neutPerc;

STORE Perc INTO '/sentiment/pigOutput' USING PigStorage('\t');



