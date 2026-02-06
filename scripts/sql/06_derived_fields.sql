-- Step 6: Compute Derived Fields
-- Assignment requirement: primary_portfolio and years_of_experience

UPDATE `coffeespace-sandbox.coffeespace_canonical.people_canonical`
SET derived_fields = STRUCT(
  -- Primary portfolio based on headline keywords
  CASE
    WHEN LOWER(identity.headline) LIKE '%software%'
      OR LOWER(identity.headline) LIKE '%engineer%'
      OR LOWER(identity.headline) LIKE '%developer%'
      OR LOWER(identity.headline) LIKE '%swe%'
      OR LOWER(identity.headline) LIKE '%backend%'
      OR LOWER(identity.headline) LIKE '%frontend%'
      OR LOWER(identity.headline) LIKE '%full stack%'
      OR LOWER(identity.headline) LIKE '%fullstack%'
      THEN 'Software Engineering'
    WHEN LOWER(identity.headline) LIKE '%data scien%'
      OR LOWER(identity.headline) LIKE '%machine learning%'
      OR LOWER(identity.headline) LIKE '%ml engineer%'
      OR LOWER(identity.headline) LIKE '%data analyst%'
      OR LOWER(identity.headline) LIKE '%analytics%'
      THEN 'Data Science'
    WHEN LOWER(identity.headline) LIKE '%product manag%'
      OR LOWER(identity.headline) LIKE '%product lead%'
      OR LOWER(identity.headline) LIKE '%product owner%'
      THEN 'Product Management'
    WHEN LOWER(identity.headline) LIKE '%design%'
      OR LOWER(identity.headline) LIKE '%ux%'
      OR LOWER(identity.headline) LIKE '%ui%'
      OR LOWER(identity.headline) LIKE '%creative%'
      THEN 'Design'
    WHEN LOWER(identity.headline) LIKE '%sales%'
      OR LOWER(identity.headline) LIKE '%account exec%'
      OR LOWER(identity.headline) LIKE '%business develop%'
      OR LOWER(identity.headline) LIKE '%bdr%'
      THEN 'Sales'
    WHEN LOWER(identity.headline) LIKE '%marketing%'
      OR LOWER(identity.headline) LIKE '%growth%'
      OR LOWER(identity.headline) LIKE '%brand%'
      OR LOWER(identity.headline) LIKE '%content%'
      THEN 'Marketing'
    WHEN LOWER(identity.headline) LIKE '%finance%'
      OR LOWER(identity.headline) LIKE '%accounting%'
      OR LOWER(identity.headline) LIKE '%fp&a%'
      OR LOWER(identity.headline) LIKE '%controller%'
      THEN 'Finance'
    WHEN LOWER(identity.headline) LIKE '%hr %'
      OR LOWER(identity.headline) LIKE '%human resources%'
      OR LOWER(identity.headline) LIKE '%recruiter%'
      OR LOWER(identity.headline) LIKE '%talent%'
      OR LOWER(identity.headline) LIKE '%people ops%'
      THEN 'Human Resources'
    WHEN LOWER(identity.headline) LIKE '%operations%'
      OR LOWER(identity.headline) LIKE '%ops manager%'
      OR LOWER(identity.headline) LIKE '%logistics%'
      OR LOWER(identity.headline) LIKE '%supply chain%'
      THEN 'Operations'
    WHEN LOWER(identity.headline) LIKE '%ceo%'
      OR LOWER(identity.headline) LIKE '%cto%'
      OR LOWER(identity.headline) LIKE '%cfo%'
      OR LOWER(identity.headline) LIKE '%coo%'
      OR LOWER(identity.headline) LIKE '%founder%'
      OR LOWER(identity.headline) LIKE '%co-founder%'
      OR LOWER(identity.headline) LIKE '%vp %'
      OR LOWER(identity.headline) LIKE '%vice president%'
      OR LOWER(identity.headline) LIKE '%director%'
      OR LOWER(identity.headline) LIKE '%head of%'
      THEN 'Executive'
    ELSE 'Other'
  END AS primary_portfolio,

  -- Years of experience (sum of experience durations)
  (
    SELECT ROUND(SUM(
      DATE_DIFF(
        COALESCE(exp.end_date, CURRENT_DATE()),
        exp.start_date,
        DAY
      ) / 365.25
    ), 1)
    FROM UNNEST(experience) AS exp
    WHERE exp.start_date IS NOT NULL
  ) AS years_of_experience,

  'v1: headline_keywords + experience_date_math' AS computation_method
)
WHERE TRUE;
