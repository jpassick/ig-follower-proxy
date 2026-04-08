// api/generate-analysis.js

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { prompt } = req.body;
  if (!prompt) {
    return res.status(400).json({ error: 'Prompt is required' });
  }

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-3-5-sonnet-latest',
        max_tokens: 500,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const data = await response.json();
    
    if (!response.ok) {
        throw new Error(data.error?.message || 'Anthropic API error');
    }

    // Extract the text from Claude's response
    const text = data.content?.filter(b => b.type === 'text').map(b => b.text).join('') || '';
    
    return res.status(200).json({ text });
    
  } catch (err) {
    console.error('AI Generation Error:', err);
    return res.status(500).json({ error: err.message });
  }
}
