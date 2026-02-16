import { ArrowRight, Play } from 'lucide-react'
import { RushTILogo } from './logos/RushTILogo'
import AnimatedIDE from './AnimatedIDE'

export default function Hero() {

  return (
    <section className="relative flex items-center justify-center overflow-x-hidden pt-16 pb-16 bg-gradient-to-b from-white to-slate-50">
      {/* Background gradient orbs - very subtle on light theme */}
      <div className="absolute w-[600px] h-[600px] rounded-full bg-sky-400/5 blur-3xl -top-40 -left-40 animate-float" />
      <div className="absolute w-[500px] h-[500px] rounded-full bg-amber-400/5 blur-3xl top-1/2 -right-40 animate-float-delayed" />
      <div className="absolute w-[400px] h-[400px] rounded-full bg-sky-400/5 blur-3xl bottom-0 left-1/3 animate-float" style={{ animationDelay: '5s' }} />

      <div className="relative z-10 w-full">
        {/* Content Container */}
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20">
          <div className="text-center">
            {/* Big RushTI Logo above the headline */}
            <div className="flex justify-center mb-8">
              <RushTILogo height={80} className="drop-shadow-lg" />
            </div>

            {/* Badge */}
            <div className="inline-flex items-center space-x-2 bg-white border border-slate-200 rounded-full px-4 py-2 mb-8 shadow-sm">
              <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
              <span className="text-slate-600 text-sm">Open Source & Free</span>
            </div>

            {/* Main headline */}
            <h1 className="text-4xl sm:text-5xl md:text-6xl lg:text-7xl font-bold mb-6 leading-tight">
              <span className="text-slate-900">Parallel TI Execution</span>
              <br />
              <span className="bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent">for IBM Planning Analytics</span>
            </h1>

            {/* Subheadline */}
            <p className="text-slate-600 text-lg sm:text-xl max-w-3xl mx-auto mb-10 leading-relaxed">
              RushTI transforms your TurboIntegrator processes into intelligent,
              parallel workflows. Execute faster with DAG-based scheduling,
              self-optimization, and fault-tolerant checkpoints.
            </p>

            {/* CTA Buttons - Orange primary for strong contrast */}
            <div className="flex flex-col sm:flex-row items-center justify-center space-y-4 sm:space-y-0 sm:space-x-4 mb-8">
              <a
                href="/rushti/docs/getting-started/installation/"
                className="inline-flex items-center space-x-2 bg-amber-500 hover:bg-amber-600 text-white px-6 py-3 rounded-lg font-semibold transition-all hover:shadow-lg hover:shadow-amber-500/25 hover:-translate-y-0.5"
              >
                <span>Get Started</span>
                <ArrowRight className="w-4 h-4" />
              </a>
              <a
                href="/rushti/docs/"
                className="inline-flex items-center space-x-2 bg-white border border-slate-300 text-slate-700 hover:border-sky-500 hover:text-sky-600 px-6 py-3 rounded-lg font-medium transition-all"
              >
                <Play className="w-4 h-4" />
                <span>View Documentation</span>
              </a>
            </div>
          </div>
        </div>

        {/* Hero visualization - Animated IDE (full width, breaks out of container) */}
        <div className="mt-8 relative w-full px-4 pb-8 pl-8 lg:pl-16">
          <div className="absolute inset-0 bg-gradient-to-t from-slate-50 via-transparent to-transparent z-10 pointer-events-none h-48 bottom-0 top-auto" />
          <AnimatedIDE />
        </div>
      </div>
    </section>
  )
}
