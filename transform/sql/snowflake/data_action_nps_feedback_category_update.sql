UPDATE analytics.mattermost.nps_feedback_classification
    SET category = recent_updates.category
    FROM (
        SELECT 
            last_feedback_date
          , server_id
          , user_id
          , subcategory
          , CASE 
                 WHEN subcategory IN ('Audio/Video Calling', 'Away Status', 'Block User', 'Channel Sidebar',
                                      'Corporate Slack Import', 'Custom Statuses', 'Emoji/GIF Options',
                                      'Group Mentions', 'Integrations', 'Mark as Unread', 'Message Tagging',
                                      'Miscellaneous Features', 'Read Receipts', 'Reminderbot', 'RN Multi-Server',
                                      'Share Message', 'Snippets', 'Text Editor', 'Threading', 'Voice Messages')
                                                               THEN 'Feature Requests'
                 WHEN subcategory IN ('Invalid')
                                                               THEN 'Invalid'
                 WHEN subcategory IN ('Miscellaneous')
                                                               THEN 'Miscellaneous'
                 WHEN subcategory IN ('Praise')
                                                               THEN 'Praise'
                 WHEN subcategory IN ('Desktop Stability', 'Message and Network Reliability',
                                      'Miscellaneous Bugs', 'Mobile Stability', 'Notifications Reliability')
                                                               THEN 'Reliability'
                 WHEN subcategory IN ('Account Authorization', 'Be Slack', 'Desktop UX', 'Documentation',
                                      'Files/Attachments', 'Group Message UX', 'Logout Issues', 'Messaging UX',
                                      'Miscellaneous UX', 'Mobile UX', 'Notifications UX', 'Onboarding', 'Performance',
                                      'Search UX', 'System Administration', 'Team Management', 'Themes', 'Translations',
                                      'UI/UX Polish', 'Update UX')
                                                               THEN 'UX Feedback'
                 ELSE category END AS category
         , categorized_at
         , categorized_by
        FROM analytics.mattermost.nps_feedback_classification
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    ) AS recent_updates
    WHERE nps_feedback_classification.last_feedback_date = recent_updates.last_feedback_date
        AND nps_feedback_classification.user_id = recent_updates.user_id
        AND nps_feedback_classification.server_id = recent_updates.server_id
        AND nps_feedback_classification.subcategory = recent_updates.subcategory
        AND nps_feedback_classification.categorized_at = recent_updates.categorized_at
        AND nps_feedback_classification.categorized_by = recent_updates.categorized_by;

UPDATE analytics.mattermost.nps_feedback_classification 
   SET category_rank = a.category_ranking
   FROM (
      SELECT *
         , ROW_NUMBER() OVER 
            (PARTITION BY LAST_FEEDBACK_DATE, SERVER_ID, USER_ID ORDER BY MANUALLY_CATEGORIZED DESC, CATEGORIZED_AT DESC) AS category_ranking
      FROM analytics.mattermost.nps_feedback_classification
   ) a
   WHERE a.last_feedback_date = nps_feedback_classification.last_feedback_date
   AND a.server_id = nps_feedback_classification.server_id
   AND a.user_id = nps_feedback_classification.user_id
   AND a.category = nps_feedback_classification.category
   AND a.subcategory = nps_feedback_classification.subcategory
   AND a.categorized_at = nps_feedback_classification.categorized_at
   AND a.categorized_by = nps_feedback_classification.categorized_by
   AND a.MANUALLY_CATEGORIZED = nps_feedback_classification.MANUALLY_CATEGORIZED;
