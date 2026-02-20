import { useScrollAnimation } from '../hooks/useScrollAnimation'
import { GitBranch, Zap, ArrowRight, Clock, Server, CheckCircle2, Mail } from 'lucide-react'

// Color palette for task categories
const colorMap = {
  sky: { gradient: 'from-sky-500 to-sky-400' },
  amber: { gradient: 'from-amber-500 to-amber-400' },
  emerald: { gradient: 'from-emerald-500 to-emerald-400' },
  violet: { gradient: 'from-violet-500 to-violet-400' },
  rose: { gradient: 'from-rose-500 to-rose-400' },
  slate: { gradient: 'from-slate-400 to-slate-300' },
}

// Small task bar used in both diagrams
function TaskBar({ name, duration, color, style, className = '' }) {
  const colors = colorMap[color]
  return (
    <div className={`relative ${className}`} style={style}>
      <div className={`h-8 bg-gradient-to-r ${colors.gradient} rounded-md flex items-center px-2.5 justify-between min-w-0`}>
        <span className="text-[10px] font-medium text-white truncate">{name}</span>
        <span className="text-[9px] text-white/80 font-mono flex-shrink-0 ml-1.5">{duration}</span>
      </div>
    </div>
  )
}

// Wait marker for sequential diagram
function WaitMarker({ style }) {
  return (
    <div className="flex items-center gap-3" style={style}>
      <div className="w-3" />
      <div className="flex-1 border-t-2 border-dashed border-slate-200 relative">
        <span className="absolute left-1/2 -translate-x-1/2 -top-2.5 text-[9px] text-slate-400 bg-white px-2 font-medium">wait</span>
      </div>
    </div>
  )
}

// Animated flow diagram showing sequential → parallel transformation
function TransformationDiagram({ isVisible }) {
  // Sequential: each task runs one after another
  const sequentialTasks = [
    { name: 'products.load', duration: '2s', color: 'sky' },
    { name: 'customers.load', duration: '2s', color: 'sky' },
    { type: 'wait' },
    { name: 'actuals.clear', duration: '1s', color: 'amber' },
    { type: 'wait' },
    { name: 'actuals.load.month pMonth=Jan', duration: '2s', color: 'emerald' },
    { name: 'actuals.load.month pMonth=Feb', duration: '2s', color: 'emerald' },
    { name: 'actuals.load.month pMonth=Mar', duration: '2s', color: 'emerald' },
    { name: '...8 more months', duration: '16s', color: 'slate' },
    { name: 'actuals.load.month pMonth=Dec', duration: '2s', color: 'emerald' },
    { type: 'wait' },
    { name: 'actuals.reconcile.and.mail', duration: '3s', color: 'violet' },
  ]

  return (
    <div className="grid grid-cols-1 lg:grid-cols-11 gap-6 lg:gap-4 items-center">
      {/* Before - Sequential */}
      <div
        className={`lg:col-span-5 transition-all duration-700 ${isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 -translate-x-8'}`}
        style={{ transitionDelay: '300ms' }}
      >
        <div className="bg-white rounded-2xl border border-slate-200 p-5 shadow-sm">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-amber-400" />
              <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Sequential</span>
            </div>
            <div className="flex items-center gap-1.5 px-2.5 py-1 bg-red-50 rounded-full border border-red-100">
              <Clock className="w-3 h-3 text-red-500" />
              <span className="text-xs font-bold text-red-600">32s total</span>
            </div>
          </div>

          <div className="space-y-1.5">
            {sequentialTasks.map((task, i) => {
              const animStyle = {
                opacity: isVisible ? 1 : 0,
                transform: isVisible ? 'translateX(0)' : 'translateX(-20px)',
                transition: 'all 0.4s ease-out',
                transitionDelay: `${400 + i * 60}ms`,
              }

              if (task.type === 'wait') {
                return <WaitMarker key={`wait-${i}`} style={animStyle} />
              }

              return (
                <div key={`${task.name}-${i}`} className="flex items-center gap-3" style={animStyle}>
                  <div className="w-3 text-[9px] text-slate-400 text-right">{i + 1 - sequentialTasks.slice(0, i).filter(t => t.type === 'wait').length}</div>
                  <TaskBar name={task.name} duration={task.duration} color={task.color} className="flex-1" />
                </div>
              )
            })}
          </div>

          <div className="mt-3 pt-2.5 border-t border-slate-100 text-center">
            <span className="text-[11px] text-slate-400">Each task waits for the previous one to complete</span>
          </div>
        </div>
      </div>

      {/* Arrow in the middle */}
      <div
        className={`lg:col-span-1 flex items-center justify-center transition-all duration-700 ${isVisible ? 'opacity-100 scale-100' : 'opacity-0 scale-50'}`}
        style={{ transitionDelay: '600ms' }}
      >
        <div className="flex flex-col items-center gap-2">
          <div className="w-12 h-12 rounded-full bg-gradient-to-br from-sky-500 to-cyan-500 flex items-center justify-center shadow-lg shadow-sky-500/20">
            <Zap className="w-5 h-5 text-white" />
          </div>
          <ArrowRight className="w-5 h-5 text-slate-300 rotate-90 lg:rotate-0" />
        </div>
      </div>

      {/* After - Parallel DAG */}
      <div
        className={`lg:col-span-5 transition-all duration-700 ${isVisible ? 'opacity-100 translate-x-0' : 'opacity-0 translate-x-8'}`}
        style={{ transitionDelay: '500ms' }}
      >
        <div className="bg-white rounded-2xl border border-slate-200 p-5 shadow-sm relative overflow-hidden">
          <div className="absolute -top-10 -right-10 w-40 h-40 bg-emerald-400/10 rounded-full blur-3xl" />

          <div className="relative">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-emerald-400" />
                <span className="text-xs font-semibold text-slate-500 uppercase tracking-wider">Parallel DAG</span>
              </div>
              <div className="flex items-center gap-1.5 px-2.5 py-1 bg-emerald-50 rounded-full border border-emerald-100">
                <Zap className="w-3 h-3 text-emerald-500" />
                <span className="text-xs font-bold text-emerald-600">8s total</span>
              </div>
            </div>

            {/* DAG stages */}
            <div className="space-y-3">
              {/* Stage 1: Metadata loads in parallel */}
              <div>
                <div className="flex items-center gap-1.5 mb-1.5">
                  <div className="text-[9px] font-semibold text-slate-400 uppercase tracking-wider">Stage 1</div>
                  <div className="flex-1 h-px bg-slate-100" />
                  <div className="text-[9px] text-slate-400 font-mono">2s</div>
                </div>
                <div className="grid grid-cols-2 gap-1.5">
                  {['products.load', 'customers.load'].map((name, i) => (
                    <TaskBar
                      key={name}
                      name={name}
                      duration="2s"
                      color="sky"
                      style={{
                        opacity: isVisible ? 1 : 0,
                        transform: isVisible ? 'scaleX(1)' : 'scaleX(0)',
                        transformOrigin: 'left',
                        transition: 'all 0.5s ease-out',
                        transitionDelay: `${700 + i * 100}ms`,
                      }}
                    />
                  ))}
                </div>
              </div>

              {/* Dependency arrow */}
              <div className="flex justify-center">
                <div className="flex flex-col items-center">
                  <div className="w-px h-2 bg-slate-300" />
                  <div className="w-0 h-0 border-l-[4px] border-l-transparent border-r-[4px] border-r-transparent border-t-[5px] border-t-slate-300" />
                </div>
              </div>

              {/* Stage 2: Clear data */}
              <div>
                <div className="flex items-center gap-1.5 mb-1.5">
                  <div className="text-[9px] font-semibold text-slate-400 uppercase tracking-wider">Stage 2</div>
                  <div className="flex-1 h-px bg-slate-100" />
                  <div className="text-[9px] text-slate-400 font-mono">1s</div>
                </div>
                <TaskBar
                  name="actuals.clear pYear=2025"
                  duration="1s"
                  color="amber"
                  style={{
                    opacity: isVisible ? 1 : 0,
                    transform: isVisible ? 'scaleX(1)' : 'scaleX(0)',
                    transformOrigin: 'left',
                    transition: 'all 0.5s ease-out',
                    transitionDelay: '900ms',
                  }}
                />
              </div>

              {/* Dependency arrow */}
              <div className="flex justify-center">
                <div className="flex flex-col items-center">
                  <div className="w-px h-2 bg-slate-300" />
                  <div className="w-0 h-0 border-l-[4px] border-l-transparent border-r-[4px] border-r-transparent border-t-[5px] border-t-slate-300" />
                </div>
              </div>

              {/* Stage 3: Load all months in parallel */}
              <div>
                <div className="flex items-center gap-1.5 mb-1.5">
                  <div className="text-[9px] font-semibold text-slate-400 uppercase tracking-wider">Stage 3</div>
                  <div className="flex-1 h-px bg-slate-100" />
                  <div className="text-[9px] text-slate-400 font-mono">2s</div>
                </div>
                <div className="grid grid-cols-4 gap-1">
                  {['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'].map((month, i) => (
                    <div
                      key={month}
                      className={`h-7 bg-gradient-to-r ${colorMap.emerald.gradient} rounded-md flex items-center justify-center`}
                      style={{
                        opacity: isVisible ? 1 : 0,
                        transform: isVisible ? 'scale(1)' : 'scale(0.5)',
                        transition: 'all 0.3s ease-out',
                        transitionDelay: `${1000 + i * 40}ms`,
                      }}
                    >
                      <span className="text-[9px] font-medium text-white">{month}</span>
                    </div>
                  ))}
                </div>
                <div className="text-center mt-1">
                  <span className="text-[9px] text-slate-400">12× actuals.load.month running in parallel</span>
                </div>
              </div>

              {/* Dependency arrow */}
              <div className="flex justify-center">
                <div className="flex flex-col items-center">
                  <div className="w-px h-2 bg-slate-300" />
                  <div className="w-0 h-0 border-l-[4px] border-l-transparent border-r-[4px] border-r-transparent border-t-[5px] border-t-slate-300" />
                </div>
              </div>

              {/* Stage 4: Reconcile & mail */}
              <div>
                <div className="flex items-center gap-1.5 mb-1.5">
                  <div className="text-[9px] font-semibold text-slate-400 uppercase tracking-wider">Stage 4</div>
                  <div className="flex-1 h-px bg-slate-100" />
                  <div className="text-[9px] text-slate-400 font-mono">3s</div>
                </div>
                <div className="flex items-center gap-2">
                  <TaskBar
                    name="actuals.reconcile.and.mail"
                    duration="3s"
                    color="violet"
                    className="flex-1"
                    style={{
                      opacity: isVisible ? 1 : 0,
                      transform: isVisible ? 'scaleX(1)' : 'scaleX(0)',
                      transformOrigin: 'left',
                      transition: 'all 0.5s ease-out',
                      transitionDelay: '1500ms',
                    }}
                  />
                  <Mail
                    className="w-4 h-4 text-violet-400 flex-shrink-0"
                    style={{
                      opacity: isVisible ? 1 : 0,
                      transition: 'opacity 0.5s ease-out',
                      transitionDelay: '1700ms',
                    }}
                  />
                </div>
              </div>
            </div>

            <div className="mt-3 pt-2.5 border-t border-slate-100 text-center">
              <span className="text-[11px] text-slate-400">Dependencies respected — independent tasks run in parallel</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default function WhatIsRushTI() {
  const [ref, isVisible] = useScrollAnimation()

  return (
    <section className="py-24 relative bg-white overflow-hidden">
      {/* Subtle background decoration */}
      <div className="absolute w-[600px] h-[600px] rounded-full bg-sky-400/3 blur-3xl -top-20 -right-40" />
      <div className="absolute w-[400px] h-[400px] rounded-full bg-emerald-400/3 blur-3xl bottom-0 -left-20" />

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16" ref={ref}>
          <div
            className={`inline-flex items-center space-x-2 bg-sky-50 border border-sky-200 rounded-full px-4 py-2 mb-6 transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-4'}`}
          >
            <Zap className="w-4 h-4 text-sky-600" />
            <span className="text-sky-700 text-sm font-medium">What is RushTI?</span>
          </div>

          <h2
            className={`text-3xl sm:text-4xl lg:text-5xl font-bold text-slate-900 mb-6 leading-tight transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
            style={{ transitionDelay: '100ms' }}
          >
            Turn sequential TI processes
            <br />
            <span className="bg-gradient-to-r from-sky-600 to-cyan-500 bg-clip-text text-transparent">into parallel workflows</span>
          </h2>

          <p
            className={`text-slate-600 text-lg max-w-3xl mx-auto leading-relaxed transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
            style={{ transitionDelay: '200ms' }}
          >
            RushTI is an open-source execution engine for IBM Planning Analytics that replaces
            sequential TurboIntegrator execution with intelligent, dependency-aware parallel scheduling.
            Define your tasks, declare their dependencies, and let RushTI handle the rest.
          </p>
        </div>

        {/* Transformation diagram */}
        <div className="mb-12">
          <TransformationDiagram isVisible={isVisible} />
        </div>

        {/* Stat banner */}
        <div
          className={`grid grid-cols-2 md:grid-cols-4 gap-4 transition-all duration-700 ${isVisible ? 'opacity-100 translate-y-0' : 'opacity-0 translate-y-8'}`}
          style={{ transitionDelay: '800ms' }}
        >
          {[
            { value: 'Multi', label: 'TM1 Instance Support', icon: Server },
            { value: 'DAG', label: 'Dependency Engine', icon: GitBranch },
            { value: '100%', label: 'Backwards Compatible', icon: CheckCircle2 },
            { value: 'Free', label: 'Open Source', icon: Zap },
          ].map((stat) => (
            <div key={stat.label} className="text-center p-5 bg-slate-50 rounded-xl border border-slate-100">
              <stat.icon className="w-5 h-5 text-sky-500 mx-auto mb-2" />
              <div className="text-2xl font-bold text-slate-900">{stat.value}</div>
              <div className="text-xs text-slate-500 mt-1">{stat.label}</div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
