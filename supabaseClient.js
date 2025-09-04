const { createClient } = require('@supabase/supabase-js');

// Check if we have valid environment variables
const isValidUrl = (url) => {
  try {
    new URL(url);
    return !url.includes('your_') && url.startsWith('http');
  } catch {
    return false;
  }
};

const hasValidConfig = isValidUrl(process.env.SUPABASE_URL) && 
                      process.env.SUPABASE_KEY && 
                      !process.env.SUPABASE_KEY.includes('your_');

let supabase;

if (hasValidConfig) {
  supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_KEY
  );
} else {
  // Create a mock client for demo mode
  supabase = {
    from: () => ({
      select: () => ({ data: [], error: null }),
      insert: () => ({ data: [], error: null }),
      update: () => ({ data: [], error: null }),
      delete: () => ({ data: [], error: null }),
      eq: () => ({ data: [], error: null }),
      order: () => ({ data: [], error: null }),
    }),
  };
}

module.exports = supabase;
