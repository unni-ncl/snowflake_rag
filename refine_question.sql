CREATE OR REPLACE PROCEDURE process_conversation(conversation VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var currentQuestion = conversation.currentQuestion.question;
    var conversationHistory = conversation.conversationHistory;

    // Function to execute Cortex completion
    function cortexComplete(model, messages) {
        var sqlCommand = `SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '${model}',
            parse_json('${JSON.stringify(messages)}'),
            {}
        ) AS response;`;
        
        var stmt = snowflake.createStatement({sqlText: sqlCommand});
        var resultSet = stmt.execute();
        resultSet.next();
        var responseJSON = resultSet.getColumnValue(1);
        var response = JSON.parse(responseJSON);
        return response.choices[0].message.content.trim();
    }

    // Step 1: Check if the current question is self-contained
    var contextCheckMessages = [
        {
            'role': 'system',
            'content': 'You are a helpful assistant. Answer "Yes" if the user\'s question is self-contained and clear, or "No" if it requires additional context.'
        },
        {
            'role': 'user',
            'content': `Question: "${currentQuestion}"

Answer:`
        }
    ];

    var contextCheckAnswer = cortexComplete('llama2-70b-chat', contextCheckMessages);

    if (contextCheckAnswer.toLowerCase().startsWith('yes')) {
        // Question has enough context
        return {
            'refinedQuestion': currentQuestion,
            'refined': false
        };
    } else {
        // Step 2: Search for relevant previous Q&A
        var maxHistoryToCheck = 5;
        var refined = false;

        for (var i = conversationHistory.length - 1; i >= 0 && (conversationHistory.length - i) <= maxHistoryToCheck; i--) {
            var previousQA = conversationHistory[i];

            // Check relevance of previous Q&A
            var relevanceCheckMessages = [
                {
                    'role': 'system',
                    'content': 'Determine if the previous exchange is relevant to understanding the current question. Answer "Yes" or "No".'
                },
                {
                    'role': 'user',
                    'content': `Current Question: "${currentQuestion}"
Previous Question: "${previousQA.question}"
Previous Answer: "${previousQA.answer}"

Answer:`
                }
            ];

            var relevanceAnswer = cortexComplete('llama2-70b-chat', relevanceCheckMessages);

            if (relevanceAnswer.toLowerCase().startsWith('yes')) {
                // Previous Q&A is relevant
                var refinedQuestionMessages = [
                    {
                        'role': 'system',
                        'content': 'Using the previous exchange and the current question, generate a clear, self-contained question that includes necessary context. Do not include any additional text or explanations.'
                    },
                    {
                        'role': 'user',
                        'content': `Previous Question: "${previousQA.question}"
Previous Answer: "${previousQA.answer}"
Current Question: "${currentQuestion}"

Refined Question:`
                    }
                ];

                var refinedQuestion = cortexComplete('llama2-70b-chat', refinedQuestionMessages);

                refined = true;
                return {
                    'refinedQuestion': refinedQuestion,
                    'refined': refined
                };
            }
        }
        // No relevant previous Q&A found
        return {
            'refinedQuestion': currentQuestion,
            'refined': false,
            'message': 'Please provide more context in your question.'
        };
    }
$$;





// Example 3: Technical discussion (3-minute gaps)
{
    "conversation_id": "CONV_2023_003",
    "conversationHistory": [
        {
            "epochTime": 1699000000,
            "question": "What's the required sampling plan for contact resistance?",
            "answer": "Contact resistance sampling requirements: 1) Minimum 9 dies per wafer, 2) Five measurements per die, 3) Center and edge die locations required, 4) 100% of first lot, 5) 3 wafers per lot after, 6) Golden wafer every 4 hours, 7) Additional after process changes, 8) Monitor wafer with each lot."
        },
        {
            "epochTime": 1699000180,  // +180 seconds
            "question": "How do we automate the measurements?",
            "answer": "```python\nfrom fab_automation import ProberControl\n\ndef setup_cr_measurement(wafer_id):\n    prober = ProberControl()\n    sites = generate_measurement_sites(9)  # 9 dies\n    results = []\n    \n    for site in sites:\n        # 5 measurements per die\n        die_measurements = []\n        for i in range(5):\n            measurement = prober.measure_contact_resistance(site)\n            die_measurements.append(measurement)\n        \n        results.append({\n            'die_location': site,\n            'measurements': die_measurements,\n            'average': sum(die_measurements)/len(die_measurements)\n        })\n    \n    return results\n```"
        }
    ],
    "currentQuestion": {
        "epochTime": 1699000360,  // +180 seconds from last question
        "question": "How should we handle measurement errors in the script?",
        "answer": null
    }
}