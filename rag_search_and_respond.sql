-- Create error logging table
CREATE TABLE IF NOT EXISTS mygpt.profile.t_error (
    error_id NUMBER IDENTITY(1,1),
    procedure_name VARCHAR(100),
    error_message VARCHAR(5000),
    error_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    input_params VARIANT,
    PRIMARY KEY (error_id)
);

-- Create debug logging table
CREATE TABLE IF NOT EXISTS mygpt.profile.t_debug_log (
    debug_id NUMBER IDENTITY(1,1),
    request_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    service_id INTEGER,
    input_params VARIANT,
    question_summary VARCHAR(5000),
    rag_results VARIANT,
    llm_response VARCHAR(5000),
    execution_time_ms NUMBER,
    PRIMARY KEY (debug_id)
);

-- Create the procedure
CREATE OR REPLACE PROCEDURE rag_search_and_respond(INPUT_PARAMS OBJECT)
RETURNS OBJECT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
/**
 * Logs error to the t_error table
 */
function logError(errorMessage, inputParams) {
    try {
        snowflake.execute({
            sqlText: `
            INSERT INTO mygpt.profile.t_error (
                procedure_name,
                error_message,
                input_params
            )
            VALUES (?, ?, PARSE_JSON(?))`,
            binds: ['rag_search_and_respond', errorMessage, JSON.stringify(inputParams)]
        });
    } catch (logError) {
        snowflake.execute({
            sqlText: `SELECT system$log.error(?);`,
            binds: [`Failed to log to error table: ${logError.message}`]
        });
    }
}

/**
 * Logs debug information to t_debug_log table
 */
function logDebugInfo(serviceId, inputParams, summary, ragResults, llmResponse, startTime) {
    try {
        const executionTime = new Date().getTime() - startTime;
        snowflake.execute({
            sqlText: `
            INSERT INTO mygpt.profile.t_debug_log (
                service_id,
                input_params,
                question_summary,
                rag_results,
                llm_response,
                execution_time_ms
            )
            VALUES (?, PARSE_JSON(?), ?, PARSE_JSON(?), ?, ?)`,
            binds: [
                serviceId,
                JSON.stringify(inputParams),
                summary,
                JSON.stringify(ragResults),
                llmResponse,
                executionTime
            ]
        });
    } catch (logError) {
        snowflake.execute({
            sqlText: `SELECT system$log.error(?);`,
            binds: [`Failed to log debug info: ${logError.message}`]
        });
    }
}

try {
    // Record start time for debug logging
    const startTime = new Date().getTime();

    // Input validation
    if (typeof INPUT_PARAMS !== 'object' || INPUT_PARAMS === null) {
        throw new Error("Input must be a non-null object");
    }

    const service_id = INPUT_PARAMS.service_id;
    const latest_prompts = INPUT_PARAMS.latest_prompts;
    const debug = INPUT_PARAMS.debug || false;

    // Validate service_id
    if (!service_id) {
        throw new Error("Missing required parameter: service_id");
    }
    if (isNaN(Number(service_id)) || !Number.isInteger(Number(service_id)) || Number(service_id) <= 0) {
        throw new Error("service_id must be a positive integer");
    }

    // Validate latest_prompts
    if (!latest_prompts || typeof latest_prompts !== 'object') {
        throw new Error("latest_prompts must be a non-null object");
    }
    const promptKeys = Object.keys(latest_prompts);
    if (promptKeys.length === 0) {
        throw new Error("latest_prompts must contain at least one prompt");
    }
    if (promptKeys.length > 20) {
        throw new Error("latest_prompts cannot contain more than 20 prompts");
    }

    // Step 1: Get the RAG service name
    let rag_service_name;
    const service_result = snowflake.execute({
        sqlText: `
        SELECT fq_rag_service_name
        FROM mygpt.profile.t_service_registry
        WHERE service_id = ?
          AND is_active = true
        LIMIT 1
        `,
        binds: [service_id]
    });
    
    if (service_result.next()) {
        rag_service_name = service_result.getColumnValue(1);
    } else {
        throw new Error(`No active service found for service_id: ${service_id}`);
    }

    // Step 2: Process and sort prompts
    const sortedKeys = Object.keys(latest_prompts).sort((a, b) => Number(a) - Number(b));
    const sortedQuestions = sortedKeys.map(key => latest_prompts[key]);
    const summarization_prompt = `Summarize the following questions, focusing on the context relevant to the last question. Emphasize the content of the last question in your summary:\n\n${sortedQuestions.join('\n')}`;

    // Step 3: Generate question summary
    let question_summary;
    const summary_result = snowflake.execute({
        sqlText: `
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'system', 'content', 'You are a helpful AI assistant specializing in summarizing questions.'),
                OBJECT_CONSTRUCT('role', 'user', 'content', ?)
            ),
            OBJECT_CONSTRUCT()
        ) as response`,
        binds: [summarization_prompt]
    });
    
    if (summary_result.next()) {
        question_summary = summary_result.getColumnValue(1);
    } else {
        throw new Error("Failed to generate question summary");
    }

    // Step 4: Perform RAG search
    let rag_results;
    const search_result = snowflake.execute({
        sqlText: `
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                ?,
                OBJECT_CONSTRUCT(
                    'query', ?,
                    'columns', ARRAY_CONSTRUCT('transcript_text', 'region'),
                    'limit', 3
                )
            )
        )['results'] as results`,
        binds: [rag_service_name, question_summary]
    });
    
    if (search_result.next()) {
        rag_results = search_result.getColumnValue(1);
    } else {
        throw new Error("Failed to perform RAG search");
    }

    // Step 5: Generate final response
    const last_question = latest_prompts[sortedKeys[sortedKeys.length - 1]];
    const response_prompt = `
    Context: ${question_summary}
    RAG Search Results: ${JSON.stringify(rag_results)}
    User Question: ${last_question}
    Please provide a helpful response based on the context, RAG search results, and the user's question.`;

    let llm_response;
    const response_result = snowflake.execute({
        sqlText: `
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama2-70b-chat',
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT('role', 'system', 'content', 'You are a helpful AI assistant. Use the provided context and search results to answer the user''s question.'),
                OBJECT_CONSTRUCT('role', 'user', 'content', ?)
            ),
            OBJECT_CONSTRUCT()
        ) as response`,
        binds: [response_prompt]
    });
    
    if (response_result.next()) {
        llm_response = response_result.getColumnValue(1);
    } else {
        throw new Error("Failed to generate response");
    }

    // Log debug information if debug flag is true
    if (debug) {
        logDebugInfo(
            service_id,
            INPUT_PARAMS,
            question_summary,
            rag_results,
            llm_response,
            startTime
        );
    }

    return {
        llm_response: llm_response,
        question_summary: question_summary
    };
} catch (error) {
    // Log error to table
    logError(error.message, INPUT_PARAMS);
    throw error;
}
$$;

-- Example usage:
CALL rag_search_and_respond(
    OBJECT_CONSTRUCT(
        'service_id', 1,
        'debug', true,
        'latest_prompts', OBJECT_CONSTRUCT(
            '1698765432', 'What is inflation?',
            '1698765532', 'How does inflation affect the economy?',
            '1698765632', 'What are the current inflation trends?'
        )
    )
);

-- View results:
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- View debug logs:
SELECT 
    debug_id,
    request_timestamp,
    service_id,
    input_params,
    question_summary,
    rag_results,
    llm_response,
    execution_time_ms
FROM mygpt.profile.t_debug_log
ORDER BY request_timestamp DESC
LIMIT 5;

-- View errors if any:
SELECT *
FROM mygpt.profile.t_error
ORDER BY error_timestamp DESC
LIMIT 5;
