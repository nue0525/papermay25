# Prompt: Build a Prompt Builder Studio (React + Vite)

## Goal
Build a modern, light-themed React/Vite single-page application called **Prompt Builder Studio** that transforms rough user ideas into structured, production-grade prompts. The app runs on port 4000 and integrates with OpenAI, Anthropic Claude, or an Enterprise AI Gateway to generate structured prompts with Goal, Instructions, Constraints, Output Format, Example Usage, and a Complete System Prompt.

## Instructions

### 1. Project Setup
- Initialize a React + Vite project (`npm create vite@latest prompt-builder -- --template react`)
- Configure `vite.config.js` to serve on port 4000 with `host: true`
- Use Google Fonts: **Roboto** (titles), **DM Sans** (body), **JetBrains Mono** (code/monospace)
- Design a warm light theme with CSS custom properties (sand/teal palette)

### 2. Core UI Components
- **Header**: Sticky top bar with app logo/name, AI provider dropdown selector, and settings gear icon
- **Settings Panel**: Collapsible panel for API key input (password field) and optional gateway URL. Persisted in localStorage
- **Input Section**: Large `<textarea>` (not textbox) for entering raw prompt ideas. Show character/word count. Include quick-fill example buttons below (e.g., "Customer support agent", "Code review assistant")
- **Generate Button**: Primary CTA below the textarea with gradient background, loading spinner during API calls, disabled when empty. Support Ctrl+Enter keyboard shortcut
- **Result Section**: Display structured output in individual cards:
  - **Goal** (target icon, teal accent)
  - **Instructions** (list icon, blue accent, numbered items)
  - **Constraints** (shield icon, amber accent, numbered items)
  - **Output Format** (file icon, purple accent)
  - **Example Usage** (zap icon, green accent)
  - **Complete System Prompt** (expandable/collapsible, monospace `<pre>` block)
- **Action Bar**: "Copy Full Prompt" and "Export JSON" buttons above results
- **History Section**: Show last 5 generated prompts, clickable to reload. Persisted in localStorage. Clear history button
- **Empty State**: Centered illustration with helper text and quick-start example chips

### 3. Section Cards
Each result section should be a reusable `SectionCard` component with:
- Icon + uppercase title header
- Hover border accent color + shadow elevation
- Copy button (per section)
- Edit button (pencil icon) that toggles inline textarea editing with Save button
- Staggered fade-in-up animation on render

### 4. AI Provider Integration
Support three providers, selectable via dropdown:

**Anthropic Claude:**
```
POST https://api.anthropic.com/v1/messages
Headers: x-api-key, anthropic-version: 2023-06-01, anthropic-dangerous-direct-browser-access: true
Body: { model: "claude-sonnet-4-20250514", max_tokens: 4096, system: PROMPT_TEMPLATE, messages: [...] }
```

**OpenAI GPT-4o:**
```
POST https://api.openai.com/v1/chat/completions
Headers: Authorization: Bearer {key}
Body: { model: "gpt-4o", response_format: { type: "json_object" }, messages: [...] }
```

**Enterprise AI Gateway:**
```
POST {custom_gateway_url}
Headers: Authorization: Bearer {key} (optional)
Body: { model: "default", messages: [...] }
Response: OpenAI-compatible or Anthropic-compatible JSON
```

### 5. System Prompt for the AI
Use this meta-prompt to instruct the AI provider:
```
You are an expert prompt engineer. Given a user's raw idea or description, transform it into a well-structured, production-grade prompt.

Return ONLY valid JSON (no markdown, no backticks) with this exact structure:
{
  "goal": "A clear, specific statement of what this prompt aims to achieve",
  "instructions": ["Step 1...", "Step 2...", "Step 3..."],
  "constraints": ["Constraint 1...", "Constraint 2..."],
  "output_format": "Description of the expected output format and structure",
  "example_usage": "A brief example of how to use this prompt",
  "system_prompt": "The complete, ready-to-use system prompt combining all sections"
}
```

### 6. Animations & Micro-interactions
- `fadeInUp` for card entry (staggered delays: 0ms, 80ms, 160ms, 240ms, 320ms)
- `slideIn` for list items within cards
- `shimmer` gradient animation on loading skeleton placeholders
- `spin` on loading spinner in Generate button
- Hover effects: border color change + box-shadow elevation on cards
- Focus rings on inputs (2px solid accent, 2px offset)
- Smooth scroll to results after generation

### 7. State Management
All state in React hooks (`useState`, `useCallback`, `useRef`, `useEffect`):
- `input`, `provider`, `apiKey`, `gatewayUrl`, `loading`, `result`, `error`
- `showSettings`, `history`, `editingSection`, `editValue`, `showFullPrompt`, `copiedFull`
- Persist to localStorage: apiKey, provider, gatewayUrl, history (last 20 items)

## Constraints
- Single-file `App.jsx` component (no external component library)
- All icons as inline SVG components (no icon library dependency)
- All styling via inline `style` objects using CSS custom properties
- No TypeScript — plain JSX only
- No external UI framework (no Tailwind, no Material UI, no Chakra)
- Must handle API errors gracefully with visible error messages
- Textarea only (not input textbox) for prompt entry
- Light theme only (warm sand background `#FAFAF7`, teal accent `#0D9488`)
- Responsive layout with max-width constraint (820px)

## Output Format
```
prompt-builder/
├── index.html          # Entry point with Google Fonts preconnect
├── vite.config.js      # Vite config (port 4000, host: true)
├── package.json        # Dependencies: react, react-dom, @vitejs/plugin-react, vite
├── src/
│   ├── main.jsx        # React root mount
│   ├── index.css       # CSS variables, keyframes, global resets
│   └── App.jsx         # Complete application (Icons, SectionCard, LoadingSkeleton, App)
```

## How to Run
```bash
cd prompt-builder
npm install
npm run dev
# → http://localhost:4000
```

Then click the gear icon → add your API key → type your idea → click Generate Prompt.
