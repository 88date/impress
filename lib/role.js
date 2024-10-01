'use strict';

const checkRoles = (context, allowedRoles) => {
  const role = context?.session?.state?.role;
  return role && allowedRoles.includes(role);
};

const checkSegments = (context, allowedSegments) => {
  const segments = context?.session?.state?.segments;

  if (!segments || segments.length === 0) return false;

  for (const allowedSegment of allowedSegments) {
    for (const segment of segments) {
      if (allowedSegment === segment) {
        return true;
      }
    }
  }

  return false;
};

module.exports = Object.freeze({ checkRoles, checkSegments });
