import { Terminal, BookOpen, Github, Package } from 'lucide-react'
import { useState } from 'react'
import { useScrollAnimation } from '../hooks/useScrollAnimation'

export default function GetStarted() {
  const [copied, setCopied] = useState(false)
  const [ref, isVisible] = useScrollAnimation()

  const handleCopy = () => {
    navigator.clipboard.writeText('pip install rushti')
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <section className="py-24 relative overflow-hidden bg-gradient-to-b from-white to-slate-50">
      {/* Background gradient orbs */}
      <div className="absolute w-[600px] h-[600px] rounded-full bg-sky-400/5 blur-3xl top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2" />

      <div className="relative z-10 max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center" ref={ref}>
        <h2 className={`text-3xl sm:text-4xl font-bold text-slate-900 mb-4 animate-on-scroll ${isVisible ? 'visible' : ''}`}>
          Get Started in Seconds
        </h2>
        <p className={`text-slate-600 text-lg mb-10 animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '100ms' }}>
          Install RushTI and start optimizing your IBM PA workflows today.
        </p>

        {/* Install command */}
        <div className={`mb-12 animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '200ms' }}>
          <div
            onClick={handleCopy}
            className="inline-flex items-center space-x-4 bg-slate-900 rounded-xl px-6 py-4 cursor-pointer hover:bg-slate-800 transition-colors group shadow-lg"
          >
            <Terminal className="w-5 h-5 text-amber-500" />
            <code className="text-lg text-white font-mono">pip install rushti</code>
            <span className="text-slate-400 text-sm group-hover:text-amber-500 transition-colors">
              {copied ? 'Copied!' : 'Click to copy'}
            </span>
          </div>
        </div>

        {/* Links */}
        <div className={`grid grid-cols-1 sm:grid-cols-3 gap-4 max-w-2xl mx-auto animate-on-scroll ${isVisible ? 'visible' : ''}`} style={{ transitionDelay: '300ms' }}>
          <a
            href="/rushti/docs/"
            className="flex items-center justify-center space-x-2 bg-white border border-slate-200 rounded-xl px-6 py-4 hover:border-sky-500 hover:shadow-md transition-all group"
          >
            <BookOpen className="w-5 h-5 text-slate-400 group-hover:text-sky-600 transition-colors" />
            <span className="text-slate-700">Documentation</span>
          </a>
          <a
            href="https://github.com/cubewise-code/rushti"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-center space-x-2 bg-white border border-slate-200 rounded-xl px-6 py-4 hover:border-slate-400 hover:shadow-md transition-all group"
          >
            <Github className="w-5 h-5 text-slate-400 group-hover:text-slate-700 transition-colors" />
            <span className="text-slate-700">GitHub</span>
          </a>
          <a
            href="https://pypi.org/project/rushti/"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-center space-x-2 bg-white border border-slate-200 rounded-xl px-6 py-4 hover:border-amber-500 hover:shadow-md transition-all group"
          >
            <Package className="w-5 h-5 text-slate-400 group-hover:text-amber-500 transition-colors" />
            <span className="text-slate-700">PyPI</span>
          </a>
        </div>
      </div>
    </section>
  )
}
