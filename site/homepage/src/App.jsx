import Navigation from './components/Navigation'
import Hero from './components/Hero'
import Features from './components/Features'
import SqliteShowcase from './components/SqliteShowcase'
import OptimizationShowcase from './components/OptimizationShowcase'
import HowItWorks from './components/HowItWorks'
import PAIntegration from './components/PAIntegration'
import ExclusiveMode from './components/ExclusiveMode'
import UseCases from './components/UseCases'
import GetStarted from './components/GetStarted'
import WhatIsRushTI from './components/WhatIsRushTI'
import Footer from './components/Footer'

function App() {
  return (
    <div className="min-h-screen bg-white overflow-x-hidden">
      <Navigation />
      <main>
        <Hero />
        <WhatIsRushTI />
        <OptimizationShowcase />
        <SqliteShowcase />
        <ExclusiveMode />
        <HowItWorks />
        <PAIntegration />
        <Features />
        <UseCases />
        <GetStarted />
      </main>
      <Footer />
    </div>
  )
}

export default App
