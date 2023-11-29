WITH ranked_videos AS (
    SELECT 
        CHANNEL_ID,
        VIDEO_ID,
        TRENDING_DATE,
        LEAD(TRENDING_DATE, 1) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS next_trending_date,
        LAG(TRENDING_DATE, 1) OVER (PARTITION BY CHANNEL_ID, VIDEO_ID ORDER BY TRENDING_DATE) AS prev_trending_date,
        VIEW_COUNT,
        COMMENT_COUNT,
        LIKES,
        DISLIKES
    FROM dim_trending_youtube_videos_metrics
),

trending_streaks AS (
    SELECT 
        CHANNEL_ID,
        VIDEO_ID,
        TRENDING_DATE,
        next_trending_date,
        VIEW_COUNT AS view_count_end_streak,
        COMMENT_COUNT AS comment_count_end_streak,
        LIKES AS likes_end_streak,
        DISLIKES AS dislikes_end_streak
    FROM ranked_videos
    WHERE next_trending_date IS NULL OR DATEDIFF(day, TRENDING_DATE, next_trending_date) > 1
),

trending_resumes AS (
    SELECT 
        CHANNEL_ID,
        VIDEO_ID,
        TRENDING_DATE AS trending_date_resume,
        prev_trending_date,
        VIEW_COUNT AS view_count_resume,
        COMMENT_COUNT AS comment_count_resume,
        LIKES AS likes_resume,
        DISLIKES AS dislikes_resume
    FROM ranked_videos
    WHERE prev_trending_date IS NULL OR DATEDIFF(day, prev_trending_date, TRENDING_DATE) > 1
)

SELECT 
    streaks.CHANNEL_ID,
    streaks.VIDEO_ID,
    streaks.TRENDING_DATE AS last_trending_date_streak,
    resumes.trending_date_resume AS first_trending_date_resume,
    resumes.view_count_resume - streaks.view_count_end_streak AS view_count_change,
    resumes.comment_count_resume - streaks.comment_count_end_streak AS comment_count_change,
    COALESCE(resumes.likes_resume, 0) - COALESCE(streaks.likes_end_streak, 0) AS likes_change,
    COALESCE(resumes.dislikes_resume, 0) - COALESCE(streaks.dislikes_end_streak, 0) AS dislikes_change
FROM trending_streaks streaks
JOIN trending_resumes resumes ON streaks.CHANNEL_ID = resumes.CHANNEL_ID 
    AND streaks.VIDEO_ID = resumes.VIDEO_ID 
    AND streaks.TRENDING_DATE < resumes.trending_date_resume
