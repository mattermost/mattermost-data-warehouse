INSERT INTO analytics.mattermost.nps_feedback_classification(last_feedback_date, server_id, user_id, category, subcategory, categorized_at, categorized_by, manually_categorized)
SELECT
             last_feedback_date
           , server_id
           , user_id
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
                 WHEN subcategory IN ('Manual Input Required') THEN 'Manual Input Required'
                                                               ELSE NULL END AS category
           , subcategory
           , categorized_at
           , 'Automated Categorization'                                      AS categorized_by
           , FALSE                                                           AS manually_categorized
         FROM (
                  SELECT
                      n1.last_feedback_date
                    , n1.server_id
                    , n1.user_id
                    , CASE
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)oaut(h|h2|hs|hentication)(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Account Authorization'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)password(s|^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Account Authorization'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)authenticat(e|ion|ed|es|ed|ing|or|er)(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Account Authorization'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(sso|single sign on|single-sign-on|single signon|single sign-on|single-sign on)(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Account Authorization'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(quit|stop) (bugging|annoying|spamming|surveying) me(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Invalid'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mobile(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(i-p|ip|p)hone(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ios(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)android(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cras(h|hing|hes|hed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bug(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stable(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)work(s|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stability(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fi(x|xes|xing|xed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bad(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)terribl(e|y)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cra(p|ppy)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)strange(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Mobile Stability'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)notification(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)do not work(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)work(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 (REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)well(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)come thr(u|ough|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not appear(ing|s|ring|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sen(d|ds|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)don\'t work(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not working(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fail(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fix(es||\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)reliable(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)reliability(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not getting(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)don\'t get(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)dont get(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)appear(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)dont work(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)deliver(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)unable(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not sent(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not sending(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(getting|receiving|get no)(|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)pop up(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(broken|busted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)con(si|i)st(ent|ant)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)missing(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Notifications Reliability'
                          WHEN ((REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)desktop(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)linu(x|xs|x\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mac(os|osx| os| osx|-os|-osx)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)blank(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)white(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)freez(e|es|ing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)frozen(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cras(h|hing|hes|hed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bug(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stable(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)suppor(t|ting|ts|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stabil(e|ize|izing|zed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)loa(d|ds|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fix(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(blank|white)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(page|window|white page|white window|screen)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL))
                              THEN 'Desktop Stability'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stand(\-alone| alone|alone)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ap(p|ps|plication|plications)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Desktop UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(sso|single sign on|single-sign-on|single signon|single sign-on|oauth|oauth2|authorization|authentication|' ||
                                             'authenticate|authorize|authenticating|authorizing|authenticates|authenticated|authorizes|authorized|thread|threading|giphy|gif|emoji|emoticon)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)disconnect(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)stab(il|l)(ity|ize|e|izing|ized)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 (REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fai(l|led|ls|ling)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not sen(d|t|ds|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)break down(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)load(ing|s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)connection(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)delayed(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sync(s|hronization|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)network(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not send(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)d(oes not|oesn\'t|id not|idn\'t) work(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)reliabil(a|i)ty(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ski(p|ps|pping|pped)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(isn\'t|not|won\'t) appea(r|rs|ring|red)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Message and Network Reliability'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bug(s|gy|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Miscellaneous Bugs'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)error(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Miscellaneous Bugs'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cras(h|hing|hes|hed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Miscellaneous Bugs'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)threa(d|ds|ding|ded)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Threading'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)repl(ies|ying|y|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)answe(r|rs|ring|red)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)chat(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)post(s|ed|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Threading'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sidebar(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Channel Sidebar'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)chat(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)layout(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)delete(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)find(ing|s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Channel Sidebar'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)channel(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)notification(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fin(d|ding|ds)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)list(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)make(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)lot(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)favorit(e|es|ed|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sor(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(hard|difficult)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 (REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fin(d|ding|ds)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)se(e|eing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)locat(e|ing|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)vis(a|i)bil(a|i)ty(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)group(s|ing|ed|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                              --                                REGEXP_SUBSTR(LOWER(n1.feedback),
--                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)thread(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
--                                REGEXP_SUBSTR(LOWER(n1.feedback),
--                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)unread(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mute(d|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL
                              THEN 'Channel Sidebar'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)channel(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)pa(n|nn)el(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)room(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)menu(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)group(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)side(b| b)ar(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)favorit(e|ed|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)window(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 (REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)blue(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)change(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)left(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL))) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)favorit(e|es|ed|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(alphabetic|hierarchic)(al|aly|ally)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)remov(e|es|ed|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sor(t|ts|ted|ting)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)scrol(l|ls|ling|led)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)layout(s\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)colla(p|pp)s(e|es|ed|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)custo(m|mize|mized|mizes|ms)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)statu(s|ses)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(reo|o)rgani(z|s)(e|ing|ed|es|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)vis(a|i)bil(e|ity|aty)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)lis(t|ts|ting)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   --                                 REGEXP_SUBSTR(LOWER(n1.feedback),
--                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)channel(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)size(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |at |on |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)the (top|bottom|middle)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Channel Sidebar'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)grou(p|ps|ping|ped|pe)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Channel Sidebar'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)find(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Channel Sidebar'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)audi(o|ocall|calls)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)vide(o|s|call|calls)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cal(l|ls|ling)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cha(t|ts|tted|tting|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cal(l|l|ls|ling|ls|ling)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)conferenc(e|ing|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)capabilit(y|ies)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)messag(e|es|ing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
--                                     OR
--                                 REGEXP_SUBSTR(LOWER(n1.feedback),
--                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)suppor(t|ted|ts|ting)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                                   )
                              THEN 'Audio/Video Calling'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)webrtc(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Audio/Video Calling'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)scree(n|n |n-)shar(e|ing|es)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Audio/Video Calling'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(sso|single sign on|single-sign-on|single signon|single sign-on|oauth|oauth2|authorization|authentication|' ||
                                             'authenticate|authorize|authenticating|authorizing|authenticates|authenticated|authorizes|authorized|giphy|gif|emoji|emoticon)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               ((REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)integration(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)guide(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)documentation(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)integrat(e|ion|ions|ing|ed|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)plugin(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)pol(ls|ling|l)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)slash(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)git(lab|hub|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)confluence(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)jira(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)google(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)redmine(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cloudron(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)jitsi(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(3rd|third)(\-| )party(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)protocol(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)connec(t|tor|ter|ting)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(more|better) (apps|applications)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)connec(t|ted|ts|ting) (directly to|directly from|directly with|from|to|with)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Integrations'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)voice(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Voice Messages'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(/| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)audio(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)voice(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(/| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)recor(d|ds|ded|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(/| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sen(d|ds|t|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(/| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Voice Messages'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)statu(s|ses)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)emoji(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)gif(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)gi(ph|hp)y(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)emoticon(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)meme(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sticker(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Emoji/GIF Options'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)attach(es|ment|ments|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)previe(w|ws|wing|wed|wer|wor)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)file(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)phot(o|os|ographs|ograph)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)pictur(e|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)vide(o|oes|os)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)images(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Files/Attachments'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)uploa(d|ds|ding|ded)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)shar(e|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)play(back| back)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)audi(o|os)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)pla(y|yer|ys|yed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              )
                              THEN 'Files/Attachments'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)remin(d|ds|der|ders|ding)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Reminderbot'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)unread(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)watch later(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Mark as Unread'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)shar(e|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Share Message'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)forwar(d|ds|ding|ded)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Share Message'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)server(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'RN Multi-Server'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mult(i|iple|iples)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)more(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)several(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)account(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)instanc(e|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)organizatio(n|ns)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)groups(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)server(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'RN Multi-Server'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)instance(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'RN Multi-Server'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)serve(r|rs|red)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)more(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)many(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)several(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)multiple(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'RN Multi-Server'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mult(i|i |i\-)serve(r|rs|red)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'RN Multi-Server'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)d(ua|ue)l(\-| )serve(r|rs|red)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'RN Multi-Server'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)receipt(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Read Receipts'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)notif(y|ied|ies|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mar(k|ked|king|ks)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)read(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Read Receipts'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)read(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)v(ei|ie)wed(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)statu(s|ses)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Read Receipts'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bloc(k|ks|ked|king)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Block User'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ba(n|ns|nning|nned)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Block User'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)editor(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Text Editor'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)edi(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)tex(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Text Editor'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)custom statu(s|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Custom Statuses'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)statu(s|ses)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(set|update|updating|setting)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) OR
                               ((REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)custo(m|mize|mized)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)use(r|rs|r\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)statu(s|ses)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)online(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)away(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)unavailable(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)busy(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)off(line| line)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL))
                              THEN 'Custom Statuses'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)slac(k|ks|k\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)impor(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)expor(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)migrat(e|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Corporate Slack Import'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ta(g|gs|gging|gged)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Message Tagging'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)code(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Snippets'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)code(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sni(p|ps|ppet|pet|ppets|pets)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bloc(k|ks)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)past(e|es|ed|ing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Snippets'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)snippet(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Snippets'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(sso|single sign on|single-sign-on|single signon|single sign-on|oauth|oauth2|authorization|authentication|' ||
                                             'authenticate|authorize|authenticating|authorizing|authenticates|authenticated|authorizes|authorized)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)logging(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)login(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sig(n|ns|ning|ned)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)shu(tting|ts|t)( off| down| out|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(rel|l)og(ged|s|ging in|ging on|ging out| out| on|on|out|s out|s on|s in|s me out| me out| me off|s me off|ging me off|ging me out| in|in)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)kic(ked|ks|king|k)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)kicks(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)logged(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)logout(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Logout Issues'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)slow(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)performance(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cpu(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)speed(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)la(gs|g|ging|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)faster(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)memory(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)long time(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)improv(e|ing|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)speed(s|ier)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)snapp(yness|isness|ier)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fas(t|ter)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)performance(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Performance'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)lighter(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)speed(s|ier)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)snapp(yness|isness|ier)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fas(t|ter)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)client(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ap(p|ps|plication|plications)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)cras(h|hing|hes|hed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)probl(a|e)(m|ms|matic)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Performance'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)notification(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)alert(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)alarm(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sound(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)off(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)custom(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)configure(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)flash(es|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)poor(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)delay(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)priority(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mute(s|d|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bubble(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)indicat(ion|or|er|ed)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)screen(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)tone(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)snooze(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)visible(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)desktop(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)receive(d|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)display(s|ing|ed|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)show(s|ing|ed|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)experience(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)configure(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)improv(e|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Notifications UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)forma(t|ts|tting|tted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)text(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)chat(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)post(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Messaging UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)shortcu(t|ts)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Messaging UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ke(y|ys|ystroke|ystrokes)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Messaging UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)messag(e|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)limi(t|ts|ting|ted)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)characte(r|rs)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)sen(d|ds|ding|t)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)butto(n|ns)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Messaging UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)search(es||\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Search UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)desktop(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)browser(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)web(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mobile(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)app(s|lication)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)i(0|o)s(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)telegra(m|ms|m\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)android(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              AND ((REGEXP_SUBSTR(LOWER(n1.feedback),
                                                  '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                    REGEXP_SUBSTR(LOWER(n1.feedback),
                                                  '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better( messaging app| app)(lication|lications|s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NULL) OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)user experience(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)telegra(m|ms|m\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)proble(m|ms)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(sharing|handling)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ux(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)improv(e|ing|ed|es|ement|ment|ements|ments)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                   REGEXP_SUBSTR(LOWER(n1.feedback),
                                                 '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)native(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Mobile UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ux(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ui(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(less|fewer) clic(k|ks|king)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)interfac(e|es|ing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)polish(ing|es|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)des(ig|gi)(n|ns|ned|ning)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)smooth(s|er|ly|ing)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)slick(er|s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)appearance(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)streamlin(e|ing|es|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)animat(e|ion|ions|es|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)mis(s|sed) message(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Notifications UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)friendl(y|ier)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)gui(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)user experience(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(font|fonts|formatting|color|colored|colors|fancy|fancier|fanciness|fancyness|design)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'UI/UX Polish'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)admin(s|istrate|istrator|istrators|istrater|istraters|istration|istrating|istrated)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'System Administration'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)se(t|ting) up(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'System Administration'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)system(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)server(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)confi(g|gs|guration|gurations|gurating|gurate|gurated|gurates)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)settin(g|gs)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)disabl(e|ed|ing|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)ba(n|nning|nned|ns)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)bloc(k|king|ked|ks)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)delet(e|ing|ed|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'System Administration'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)system console(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'System Administration'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)manag(e|ing|ement|es)(|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)restric(t|ts|ting|ted)(|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)archiv(e|ing|es|ed)(|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)disabl(e|es|ing|ed)(|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(role|group|team|channel|permission|feature|link|video)(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'System Administration'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)electron(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Desktop UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)desktop(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)app(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)window(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)native(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Desktop UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)tutorial(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Onboarding'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)easier(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)instal(l|ling)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)joi(n|ning)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Onboarding'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)teach(^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Onboarding'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)invit(es|e|ing|ed)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Onboarding'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)onboar(ding|ded|d|ds)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Onboarding'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)grou(p|ps)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Group Message UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(add|join)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                               (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(group|channel|conversation|chat)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL)
                              THEN 'Group Message UX'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)translat(e|es|ing|ion|ions|ed|or|er|ors|ers)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Translations'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)language(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Translations'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(right to left|right-to-left)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Translations'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)(english|japanese|spanish|russian|french|german|italian|dutch|swedish|portuguese|mandarin|hindi|bengali|urdu|marathi|bavarian|czech|romanian|nepali|polish|persian|iranian|afrikaans|kurdish|greek|arabic|vietnamese|indonesian|swahili|zulu|yoruba|turkish|korean|thai|hungarian)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Translations'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)doc(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Documentation'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)document(s|ed|ation|^|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Documentation'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)guide(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Documentation'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)help(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Documentation'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)slac(k|ks|k\'s)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Be Slack'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)better( messaging app| app)(lication|lications|s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Be Slack'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)them(e|es)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Themes'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)notificatio(n|ns)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Notifications UX'
                          WHEN (REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)perfect(ed|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)i like(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)great(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)love(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)awesome(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)good(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)brilliant(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fantastic(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)any(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)issue(s|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)amazing(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)happy(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fine by me(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)seems fine(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)10/10(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)easy to use(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                REGEXP_SUBSTR(LOWER(n1.feedback),
                                              '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)nothing(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                (REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)work(s|ing|\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)well(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) OR
                                ((REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)can(not|\'t)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL OR
                                  REGEXP_SUBSTR(LOWER(n1.feedback),
                                                '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)do( not|n\'t)(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL) AND
                                 REGEXP_SUBSTR(LOWER(n1.feedback),
                                               '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)anything(\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL))
                              AND n1.score >= 7
                              THEN 'Praise'
                          WHEN LENGTH(n1.feedback) < 12
                              THEN 'Invalid'
                          WHEN LENGTH(n1.feedback) > 2000
                              THEN 'Invalid'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)fuck(s| |\\.|\;|,| |\/|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Invalid'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)surve(y|ys|ybot|y bot|y-bot)( |\\.|\;|,|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Invalid'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)not enough experience(s| |\\.|\;|,|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Invalid'
                          WHEN REGEXP_SUBSTR(LOWER(n1.feedback),
                                             '(^| |\\.|\;|,|:|\n|\t|\-|\"|\'|\/)do( not|nt|n\'t|) know(\\.| |\;|,|\n|\t|\-|\"|\'|\\?|\!|$)') IS NOT NULL
                              THEN 'Invalid'
                          WHEN REGEXP_SUBSTR(feedback,
                                             '[-a-zA-Z0-9\_\=\`\"\’\n\t\w \s\'\.\,\:\/\!\;\\\*\&\+\#\(\)\@\?\>\<\-]') IS NULL
                              THEN 'Invalid'
                              ELSE 'Manual Input Required' END AS subcategory
                    , CURRENT_TIMESTAMP                        AS categorized_at
                  FROM analytics.mattermost.nps_user_daily_score                  n1
                  LEFT JOIN analytics.mattermost.nps_feedback_classification n2
                                 ON n1.user_id = n2.user_id
                                     AND n1.server_id = n2.server_id
                                     AND n1.last_feedback_date = n2.last_feedback_date
                  WHERE n2.last_feedback_date IS NULL
                  AND n1.last_feedback_date = n1.date
                  GROUP BY 1, 2, 3, 4, 5
         )
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
