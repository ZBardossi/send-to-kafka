const produceOneByOne = async () => {
    await producer.connect();

    try {
        const data = fs.readFileSync(process.env.INPUT_FILE, 'utf8');

        const lines = data.split('\n');
        let messagesSent = 0;

        for (const line of lines) {
            try {
                const jsonContent = JSON.parse(line.trim()); // Parse existing JSON string
                const message = {
                    key: `${jsonContent.payload.sagas[0].do.pathParams.productId}:${jsonContent.payload.sagas[0].do.pathParams.sales_channel_id}`,
                    value: line.trim()
                };

                // Send each message separately
                await producer.send({
                    topic: process.env.KAFKA_TOPIC,
                    messages: [message]
                });

                messagesSent++;
                console.log(`Message ${messagesSent} sent`);
            } catch (error) {
                console.error('Error parsing JSON:', error);
                // Handle the error (e.g., log it, skip the line, etc.)
            }
        }

        console.log(`All ${messagesSent} messages sent`);
    } catch (error) {
        console.error('Error reading file:', error);
    }

    await producer.disconnect();
    console.log('Producer disconnected');
};
