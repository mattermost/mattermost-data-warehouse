UPDATE analytics.mattermost.nps_feedback_classification
    SET category = recent_updates.category
    FROM (
        SELECT 
            last_feedback_date
          , server_id
          , user_id
          , subcategory
          , CASE WHEN subcategory in ('Audio/Video Calling','Away Status','Block User','Channel Sidebar',
                                      'Corporate Slack Import','Custom Statuses','Emoji/GIF Options',
                                      'Group Mentions','Integrations','Mark as Unread','Message Tagging',
                                      'Miscellaneous Features','Read Receipts','Reminderbot','RN Multi-server',
                                      'Share Message','Snippets','Text Editor','Threading','Voice Messages') 
                    THEN 'Feature Requests'
                 WHEN subcategory in ('Invalid') 
                    THEN 'Invalid'
                 WHEN subcategory in ('Miscellaneous') 
                    THEN 'Miscellaneous'
                 WHEN subcategory in ('Praise') 
                    THEN 'Praise'
                 WHEN subcategory in ('Desktop Stability','Message and Network Reliability',
                                      'Miscellaneous Bugs','Mobile Stability','Notifications Reliability') 
                    THEN 'Reliability'
                 WHEN subcategory in ('Account Authorization','Be Slack','Desktop UX','Documentation',
                                      'Files/Attachments','Group Message UX','Logout Issues','Messaging UX',
                                      'Miscellaneous UX','Mobile UX','Notifications UX','Onboarding','Performance',
                                      'Search UX','System Administration','Team Management','Themes','Translations',
                                      'UI/UX Polish','Update UX') 
                    THEN 'UX Feedback'
                 ELSE category END AS category
        FROM analytics.mattermost.nps_feedback_classification
        GROUP BY 1, 2, 3, 4, 5
    ) AS recent_updates
    WHERE nps_feedback_classification.last_feedback_date = recent_updates.last_feedback_date
        AND nps_feedback_classification.user_id = recent_updates.user_id
        AND nps_feedback_classification.server_id = recent_updates.server_id
        AND nps_feedback_classification.subcategory = recent_updates.subcategory;