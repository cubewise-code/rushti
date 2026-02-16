import { Copy, Check } from 'lucide-react'
import { useState } from 'react'
import { useScrollAnimation } from '../hooks/useScrollAnimation'

// Pre-formatted JSON with proper syntax highlighting using JSX
function HighlightedJson() {
  const Key = ({ children }) => <span className="text-sky-600">{children}</span>
  const Str = ({ children }) => <span className="text-emerald-600">{children}</span>
  const Bracket = ({ children }) => <span className="text-slate-500">{children}</span>
  const Punct = ({ children }) => <span className="text-slate-400">{children}</span>

  return (
    <code className="text-slate-700">
      <Bracket>{'{'}</Bracket>{'\n'}
      {'  '}<Key>"tasks"</Key><Punct>:</Punct> <Bracket>[</Bracket>{'\n'}
      {'    '}<Bracket>{'{'}</Bracket>{'\n'}
      {'      '}<Key>"id"</Key><Punct>:</Punct> <Str>"load_exchange_rates"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"process"</Key><Punct>:</Punct> <Str>"Data.Load.ExchangeRates"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"parameters"</Key><Punct>:</Punct> <Bracket>{'{'}</Bracket>{'\n'}
      {'        '}<Key>"pYear"</Key><Punct>:</Punct> <Str>"2024"</Str><Punct>,</Punct>{'\n'}
      {'        '}<Key>"pSource"</Key><Punct>:</Punct> <Str>"SAP"</Str>{'\n'}
      {'      '}<Bracket>{'}'}</Bracket>{'\n'}
      {'    '}<Bracket>{'}'}</Bracket><Punct>,</Punct>{'\n'}
      {'    '}<Bracket>{'{'}</Bracket>{'\n'}
      {'      '}<Key>"id"</Key><Punct>:</Punct> <Str>"load_actuals"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"process"</Key><Punct>:</Punct> <Str>"Data.Load.Actuals"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"dependencies"</Key><Punct>:</Punct> <Bracket>[</Bracket><Str>"load_exchange_rates"</Str><Bracket>]</Bracket><Punct>,</Punct>{'\n'}
      {'      '}<Key>"parameters"</Key><Punct>:</Punct> <Bracket>{'{'}</Bracket>{'\n'}
      {'        '}<Key>"pYear"</Key><Punct>:</Punct> <Str>"2024"</Str><Punct>,</Punct>{'\n'}
      {'        '}<Key>"pPeriod"</Key><Punct>:</Punct> <Str>"12"</Str>{'\n'}
      {'      '}<Bracket>{'}'}</Bracket>{'\n'}
      {'    '}<Bracket>{'}'}</Bracket><Punct>,</Punct>{'\n'}
      {'    '}<Bracket>{'{'}</Bracket>{'\n'}
      {'      '}<Key>"id"</Key><Punct>:</Punct> <Str>"calculate_variance"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"process"</Key><Punct>:</Punct> <Str>"Calc.Variance.Analysis"</Str><Punct>,</Punct>{'\n'}
      {'      '}<Key>"dependencies"</Key><Punct>:</Punct> <Bracket>[</Bracket><Str>"load_actuals"</Str><Bracket>]</Bracket><Punct>,</Punct>{'\n'}
      {'      '}<Key>"parameters"</Key><Punct>:</Punct> <Bracket>{'{'}</Bracket>{'\n'}
      {'        '}<Key>"pScenario"</Key><Punct>:</Punct> <Str>"Actual vs Budget"</Str>{'\n'}
      {'      '}<Bracket>{'}'}</Bracket>{'\n'}
      {'    '}<Bracket>{'}'}</Bracket>{'\n'}
      {'  '}<Bracket>]</Bracket>{'\n'}
      <Bracket>{'}'}</Bracket>
    </code>
  )
}

const taskFileRaw = `{
  "tasks": [
    {
      "id": "load_exchange_rates",
      "process": "Data.Load.ExchangeRates",
      "parameters": {
        "pYear": "2024",
        "pSource": "SAP"
      }
    },
    {
      "id": "load_actuals",
      "process": "Data.Load.Actuals",
      "dependencies": ["load_exchange_rates"],
      "parameters": {
        "pYear": "2024",
        "pPeriod": "12"
      }
    },
    {
      "id": "calculate_variance",
      "process": "Calc.Variance.Analysis",
      "dependencies": ["load_actuals"],
      "parameters": {
        "pScenario": "Actual vs Budget"
      }
    }
  ]
}`

export default function TaskFilePreview() {
  const [copied, setCopied] = useState(false)
  const [ref, isVisible] = useScrollAnimation()

  const handleCopy = () => {
    navigator.clipboard.writeText(taskFileRaw)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const lineCount = taskFileRaw.split('\n').length

  return (
    <section className="py-24 relative bg-slate-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 items-center">
          {/* Text content */}
          <div ref={ref}>
            <h2 className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-6 animate-on-scroll ${isVisible ? 'visible' : ''}`}>
              Simple Configuration
            </h2>
            <p className={`text-slate-600 text-lg mb-6 leading-relaxed animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '100ms' }}>
              Define your workflows in clean, readable JSON. Specify processes, parameters, and dependencies; RushTI handles the rest.
            </p>
            <ul className={`space-y-4 animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '200ms' }}>
              <li className="flex items-start space-x-3">
                <div className="w-6 h-6 rounded-full bg-sky-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                  <Check className="w-4 h-4 text-sky-600" />
                </div>
                <span className="text-slate-600">
                  <strong className="text-slate-900">Declarative dependencies:</strong> just list what each task needs
                </span>
              </li>
              <li className="flex items-start space-x-3">
                <div className="w-6 h-6 rounded-full bg-amber-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                  <Check className="w-4 h-4 text-amber-600" />
                </div>
                <span className="text-slate-600">
                  <strong className="text-slate-900">Expandable parameters:</strong> use variables and expressions
                </span>
              </li>
              <li className="flex items-start space-x-3">
                <div className="w-6 h-6 rounded-full bg-emerald-100 flex items-center justify-center flex-shrink-0 mt-0.5">
                  <Check className="w-4 h-4 text-emerald-600" />
                </div>
                <span className="text-slate-600">
                  <strong className="text-slate-900">Validation built-in:</strong> catch errors before execution
                </span>
              </li>
            </ul>
          </div>

          {/* Code preview - Light theme */}
          <div className={`animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '300ms' }}>
            <div className="bg-white border border-slate-200 rounded-xl shadow-xl">
              {/* Window header */}
              <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200 bg-slate-50">
                <div className="flex items-center space-x-2">
                  <div className="w-3 h-3 rounded-full bg-red-400" />
                  <div className="w-3 h-3 rounded-full bg-amber-400" />
                  <div className="w-3 h-3 rounded-full bg-emerald-400" />
                  <span className="text-slate-500 text-sm ml-3 font-medium">taskfile.json</span>
                </div>
                <button
                  onClick={handleCopy}
                  className="flex items-center space-x-1 text-slate-500 hover:text-slate-700 transition-colors text-sm"
                >
                  {copied ? (
                    <>
                      <Check className="w-4 h-4 text-emerald-500" />
                      <span className="text-emerald-500">Copied!</span>
                    </>
                  ) : (
                    <>
                      <Copy className="w-4 h-4" />
                      <span>Copy</span>
                    </>
                  )}
                </button>
              </div>

              {/* Line numbers + Code */}
              <div className="flex">
                {/* Line numbers */}
                <div className="py-4 px-3 bg-slate-50 border-r border-slate-200 text-right select-none h-96">
                  {Array.from({ length: lineCount }, (_, i) => (
                    <div key={i} className="text-xs text-slate-400 leading-5 font-mono">
                      {i + 1}
                    </div>
                  ))}
                </div>

                {/* Code */}
                <pre className="p-6 text-sm overflow-x-auto flex-1 font-mono leading-5 h-96">
                  <HighlightedJson />
                </pre>
              </div>
            </div>

            {/* Floating badge */}
            <div className="flex justify-center mt-4">
              <div className="inline-flex items-center space-x-2 bg-sky-50 border border-sky-200 rounded-full px-4 py-2">
                <div className="w-2 h-2 bg-sky-500 rounded-full" />
                <span className="text-xs text-sky-700 font-medium">JSON Schema validation available</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
