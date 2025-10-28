// src/app.tsx (or wherever your App lives)
import React from 'react';
import VideosTable from '../components/VideosTable';
import PublishingTable from '../components/PublishingTable';
import { Clapperboard, Megaphone } from 'lucide-react';

export default function App() {
  return (
    <main className="max-w-7xl mx-auto px-4 py-8">
      {/* Page title */}
      <h1 className="text-3xl font-bold mb-6">Admin</h1>

      {/* Two panels: Videos + Publishing */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {/* Videos panel */}
        <section className="bg-white rounded-2xl shadow p-3">
          <h2 className="text-lg font-semibold mb-3 flex items-center gap-2">
            <Clapperboard className="h-5 w-5 text-gray-700" aria-hidden />
            <span>Videos</span>
          </h2>
          <VideosTable />
        </section>

        {/* Publishing panel */}
        <section className="bg-white rounded-2xl shadow p-3">
          <h2 className="text-lg font-semibold mb-3 flex items-center gap-2">
            <Megaphone className="h-5 w-5 text-gray-700" aria-hidden />
            <span>Publishing</span>
          </h2>
          <PublishingTable />
        </section>
      </div>
    </main>
  );
}


//commenting out test code, but leaving in place
//import TooltipTest from '../components/TooltipTest';
//function App() {
//  return (
//    <div className="p-8">
 //     <TooltipTest />
 //   </div>
 // );
//}
//export default App;
